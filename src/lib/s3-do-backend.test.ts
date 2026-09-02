import { env, runInDurableObject } from "cloudflare:test";
import { beforeEach, describe, expect, it } from "vitest";
import { DOS3Backend, DOS3Store } from "./s3-do-backend";

type TestStub = DurableObjectStub & { s3Store(): Rpc.Result<DOS3Store> };
let currentStub: TestStub;

describe("DOS3Backend", () => {
  let backend: DOS3Backend;

  beforeEach(() => {
    currentStub = env.TEST.get(env.TEST.newUniqueId()) as unknown as TestStub;
    backend = new DOS3Backend(() => currentStub.s3Store());
  });

  it("lazily caches the RPC store for the backend lifetime", async () => {
    let calls = 0;
    const cached = new DOS3Backend(() => {
      calls++;
      return currentStub.s3Store();
    });
    await cached.putObject("cached", stream("x"), { httpMetadata: {} });
    await cached.getObject("cached", {});
    await cached.listObjects({ prefix: "", limit: 10 });
    expect(calls).toBe(1);
  });

  it("applies the schema migration once and keeps it idempotent", async () => {
    await backend.listObjects({ prefix: "", limit: 0 });
    const migration = await sql<{ version: number }>(
      `
        SELECT MAX(version) AS version
          FROM s3_migrations`,
    );
    expect(migration[0]?.version).toBe(1);
    const columns = await sql<{ name: string }>(
      `
        SELECT name
          FROM pragma_table_info('s3_uploads')
      ORDER BY cid`,
    );
    expect(columns.map(({ name }) => name)).toContain("size");
    expect(columns.map(({ name }) => name)).not.toContain(["st", "ate"].join(""));
    const secondStore = currentStub.s3Store();
    await secondStore.listObjects({ prefix: "", limit: 0 });
    expect(
      (await sql<{ count: number }>("SELECT COUNT(*) AS count FROM s3_migrations"))[0]?.count,
    ).toBe(1);
  });

  it("stores an empty body", async () => {
    const object = await backend.putObject("empty", null, { httpMetadata: {} });
    expect(object.size).toBe(0);
    expect(
      await sql<{ part_number: number; size: number }>(
        `
          SELECT p.part_number,
                 p.size
            FROM s3_parts AS p
            JOIN s3_objects AS o ON o.upload_id = p.upload_id
           WHERE o.key = ?`,
        "empty",
      ),
    ).toEqual([{ part_number: 1, size: 0 }]);
    const result = await backend.getObject("empty", {});
    expect(result.kind).toBe("found");
    if (result.kind === "found") expect(await new Response(result.object.body).text()).toBe("");
  });

  it("splits a producer chunk larger than 1 MiB", async () => {
    const value = new Uint8Array(2 * 1024 * 1024 + 7).fill(65);
    await backend.putObject("large", streamBytes([value]), { httpMetadata: {} });
    const rows = await sql<{
      chunks: number;
      max_size: number;
      part_number: number;
      upload_size: number;
      chunk_size: number;
    }>(
      `
        SELECT COUNT(*) AS chunks,
               MAX(c.size) AS max_size,
               MIN(c.part_number) AS part_number,
               u.size AS upload_size,
               SUM(c.size) AS chunk_size
          FROM s3_objects AS o
          JOIN s3_uploads AS u ON u.id = o.upload_id
          JOIN s3_chunks AS c ON c.upload_id = u.id
         WHERE o.key = ?
      GROUP BY u.size`,
      "large",
    );
    expect(rows[0]?.chunks).toBeGreaterThan(1);
    expect(rows[0]?.max_size).toBeLessThanOrEqual(1024 * 1024);
    expect(rows[0]?.part_number).toBe(1);
    expect(rows[0]?.upload_size).toBe(value.byteLength);
    expect(rows[0]?.chunk_size).toBe(rows[0]?.upload_size);
    const result = await backend.getObject("large", {});
    if (result.kind !== "found") throw new Error("missing large object");
    expect((await new Response(result.object.body).arrayBuffer()).byteLength).toBe(
      value.byteLength,
    );
  });

  it("handles variable producer chunks and reads through a disposed RPC store", async () => {
    const chunks = [
      new Uint8Array([1]),
      new Uint8Array(1024 * 1024 + 3).fill(2),
      new Uint8Array([3, 4]),
    ];
    await backend.putObject("variable", streamBytes(chunks), { httpMetadata: {} });
    const result = await backend.getObject("variable", {});
    expect(result.kind).toBe("found");
    if (result.kind === "found") {
      const body = new Uint8Array(await new Response(result.object.body).arrayBuffer());
      expect(body[0]).toBe(1);
      expect(body.at(-1)).toBe(4);
      expect(body.length).toBe(1024 * 1024 + 6);
    }
  });

  it("round-trips all HTTP metadata", async () => {
    const httpMetadata = {
      contentType: "text/plain",
      contentLanguage: "ja",
      contentDisposition: "attachment",
      contentEncoding: "gzip",
      cacheControl: "max-age=60",
      cacheExpiry: new Date("2030-01-02T03:04:05Z"),
    };
    const object = await backend.putObject("metadata", stream("data"), { httpMetadata });
    expect(object.httpMetadata).toEqual(httpMetadata);
    const result = await backend.getObject("metadata", {});
    expect(result.kind).toBe("found");
    if (result.kind === "found") expect(result.object.httpMetadata).toEqual(httpMetadata);
  });

  it("overwrites an object and removes the replaced upload", async () => {
    await backend.putObject("overwrite", stream("old"), { httpMetadata: {} });
    await backend.putObject("overwrite", stream("new"), { httpMetadata: {} });
    const result = await backend.getObject("overwrite", {});
    if (result.kind !== "found") throw new Error("missing object");
    expect(await new Response(result.object.body).text()).toBe("new");
    expect((await sql("SELECT COUNT(*) AS n FROM s3_uploads WHERE key=?", "overwrite"))[0]?.n).toBe(
      1,
    );
  });

  it("preserves the old object when a producer fails", async () => {
    await backend.putObject("failed", stream("old"), { httpMetadata: {} });
    const failed = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("partial"));
        controller.error(new Error("producer failed"));
      },
    });
    await expect(backend.putObject("failed", failed, { httpMetadata: {} })).rejects.toThrow();
    const result = await backend.getObject("failed", {});
    if (result.kind !== "found") throw new Error("missing old object");
    expect(await new Response(result.object.body).text()).toBe("old");
    expect(
      (
        await sql(
          "SELECT COUNT(*) AS n FROM s3_chunks c LEFT JOIN s3_uploads u ON u.id=c.upload_id WHERE u.id IS NULL",
        )
      )[0]?.n,
    ).toBe(0);
  });

  it("deletes batches and leaves no orphan rows", async () => {
    await Promise.all([
      backend.putObject("delete/a", stream("a"), { httpMetadata: {} }),
      backend.putObject("delete/b", stream("b"), { httpMetadata: {} }),
    ]);
    await backend.deleteObjects(["delete/a", "delete/b", "delete/missing"]);
    expect((await backend.listObjects({ prefix: "delete/", limit: 10 })).objects).toHaveLength(0);
    const rows = await sql<{ uploads: number; chunks: number; parts: number }>(
      "SELECT (SELECT COUNT(*) FROM s3_uploads) AS uploads,(SELECT COUNT(*) FROM s3_chunks) AS chunks,(SELECT COUNT(*) FROM s3_parts) AS parts",
    );
    expect(rows[0]).toEqual({ uploads: 0, chunks: 0, parts: 0 });
  });

  it("applies If-Match and If-None-Match conditions", async () => {
    const object = await backend.putObject("conditions", stream("value"), { httpMetadata: {} });
    expect(
      (await backend.getObject("conditions", { conditions: { ifMatch: object.etag } })).kind,
    ).toBe("found");
    expect((await backend.getObject("conditions", { conditions: { ifMatch: "wrong" } })).kind).toBe(
      "precondition-failed",
    );
    expect(
      (await backend.getObject("conditions", { conditions: { ifNoneMatch: object.etag } })).kind,
    ).toBe("not-modified");
    expect(
      (await backend.getObject("conditions", { conditions: { ifNoneMatch: "wrong" } })).kind,
    ).toBe("found");
  });

  it("gives If-None-Match precedence over If-Modified-Since", async () => {
    const object = await backend.putObject("condition-precedence", stream("value"), {
      httpMetadata: {},
    });
    expect(
      (
        await backend.getObject("condition-precedence", {
          conditions: { ifNoneMatch: "wrong", ifModifiedSince: new Date(Date.now() + 86400000) },
        })
      ).kind,
    ).toBe("found");
    expect(
      (
        await backend.getObject("condition-precedence", {
          conditions: { ifNoneMatch: object.etag, ifModifiedSince: new Date(0) },
        })
      ).kind,
    ).toBe("not-modified");
    expect(
      (
        await backend.getObject("condition-precedence", {
          conditions: { ifModifiedSince: new Date(Date.now() + 86400000) },
        })
      ).kind,
    ).toBe("not-modified");
  });

  it("compares HTTP dates at second granularity", async () => {
    const object = await backend.putObject("condition-dates", stream("value"), {
      httpMetadata: {},
    });
    const second = Math.floor(object.uploaded.getTime() / 1000) * 1000;
    expect(
      (
        await backend.getObject("condition-dates", {
          conditions: { ifUnmodifiedSince: new Date(second + 999) },
        })
      ).kind,
    ).toBe("found");
    expect(
      (
        await backend.getObject("condition-dates", {
          conditions: { ifUnmodifiedSince: new Date(second - 1000) },
        })
      ).kind,
    ).toBe("precondition-failed");
    expect(
      (
        await backend.getObject("condition-dates", {
          conditions: { ifModifiedSince: new Date(second) },
        })
      ).kind,
    ).toBe("not-modified");
  });

  it("supports bounded, open, and suffix ranges", async () => {
    await backend.putObject("ranges", stream("0123456789"), { httpMetadata: {} });
    const cases = [
      [{ kind: "offset", offset: 2, length: 4 }, "2345", { offset: 2, length: 4 }],
      [{ kind: "offset", offset: 7 }, "789", { offset: 7, length: 3 }],
      [{ kind: "suffix", suffix: 3 }, "789", { offset: 7, length: 3 }],
    ] as const;
    for (const [range, expected, returned] of cases) {
      const result = await backend.getObject("ranges", { range });
      expect(result.kind).toBe("found");
      if (result.kind === "found") {
        expect(await new Response(result.object.body).text()).toBe(expected);
        expect(result.object.range).toEqual(returned);
      }
    }
  });

  it("rejects empty, outside, negative, zero, nonfinite, and unsafe ranges", async () => {
    await backend.putObject("invalid-ranges", stream("value"), { httpMetadata: {} });
    const ranges = [
      { kind: "offset", offset: 0, length: 0 },
      { kind: "offset", offset: -1 },
      { kind: "offset", offset: 99 },
      { kind: "offset", offset: Number.NaN },
      { kind: "offset", offset: 0, length: Number.NaN },
      { kind: "offset", offset: 0, length: Number.POSITIVE_INFINITY },
      { kind: "offset", offset: Number.MAX_SAFE_INTEGER + 1 },
      { kind: "suffix", suffix: 0 },
      { kind: "suffix", suffix: -1 },
      { kind: "suffix", suffix: Number.POSITIVE_INFINITY },
    ] as const;
    for (const range of ranges)
      expect((await backend.getObject("invalid-ranges", { range })).kind).toBe(
        "range-not-satisfiable",
      );
  });

  it("lists literal percent and underscore prefixes", async () => {
    await backend.putObject("literal%_/key", stream("x"), { httpMetadata: {} });
    const result = await backend.listObjects({ prefix: "literal%_/", limit: 10 });
    expect(result.objects.map(({ key }) => key)).toEqual(["literal%_/key"]);
  });

  it("honors startAfter", async () => {
    for (const key of ["start/a", "start/b", "start/c"])
      await backend.putObject(key, stream(key), { httpMetadata: {} });
    const result = await backend.listObjects({
      prefix: "start/",
      startAfter: "start/a",
      limit: 10,
    });
    expect(result.objects.map(({ key }) => key)).toEqual(["start/b", "start/c"]);
  });

  it("returns every key exactly once over cursor pages", async () => {
    const keys = ["pages/a", "pages/b", "pages/c", "pages/d", "pages/e"];
    for (const key of keys) await backend.putObject(key, stream(key), { httpMetadata: {} });
    const actual: string[] = [];
    let cursor: string | undefined;
    do {
      const page = await backend.listObjects({
        prefix: "pages/",
        limit: 2,
        ...(cursor !== undefined ? { cursor } : {}),
      });
      actual.push(...page.objects.map(({ key }) => key));
      cursor = page.truncated ? page.cursor : undefined;
    } while (cursor !== undefined);
    expect(actual).toEqual(keys);
  });

  it("returns each delimiter group exactly once over pages", async () => {
    for (const key of ["groups/a/1", "groups/a/2", "groups/b/1", "groups/c/1"])
      await backend.putObject(key, stream(key), { httpMetadata: {} });
    const actual: string[] = [];
    let cursor: string | undefined;
    do {
      const page = await backend.listObjects({
        prefix: "groups/",
        delimiter: "/",
        limit: 1,
        ...(cursor !== undefined ? { cursor } : {}),
      });
      actual.push(...page.delimitedPrefixes);
      cursor = page.truncated ? page.cursor : undefined;
    } while (cursor !== undefined);
    expect(actual).toEqual(["groups/a/", "groups/b/", "groups/c/"]);
    const after = await backend.listObjects({
      prefix: "groups/",
      delimiter: "/",
      startAfter: "groups/a/",
      limit: 10,
    });
    expect(after.delimitedPrefixes).toEqual(["groups/b/", "groups/c/"]);
  });

  it("paginates mixed objects and delimiter groups without duplicates", async () => {
    const keys = [
      "mixed/a",
      "mixed/b/1",
      "mixed/b/2",
      "mixed/c",
      "mixed/d/1",
      "mixed/d/2",
      "mixed/e",
    ];
    for (const key of keys) await backend.putObject(key, stream(key), { httpMetadata: {} });

    const actual: string[] = [];
    const pages: Array<{ truncated: boolean; cursor?: string }> = [];
    let cursor: string | undefined;
    do {
      const page = await backend.listObjects({
        prefix: "mixed/",
        delimiter: "/",
        limit: 1,
        ...(cursor !== undefined ? { cursor } : {}),
      });
      pages.push(page.truncated ? { truncated: true, cursor: page.cursor } : { truncated: false });
      actual.push(...page.objects.map(({ key }) => key), ...page.delimitedPrefixes);
      expect(page.objects.length + page.delimitedPrefixes.length).toBeLessThanOrEqual(1);
      cursor = page.truncated ? page.cursor : undefined;
    } while (cursor !== undefined);

    expect(actual).toEqual(["mixed/a", "mixed/b/", "mixed/c", "mixed/d/", "mixed/e"]);
    expect(new Set(actual).size).toBe(actual.length);
    expect(pages.at(-1)?.truncated).toBe(false);
    expect(
      pages.slice(0, -1).every(({ truncated, cursor: pageCursor }) => truncated && pageCursor),
    ).toBe(true);
  });

  it("supports non-BMP keys and cursors", async () => {
    const keys = ["unicode/😀/a", "unicode/😀/b", "unicode/🦄/a"];
    for (const key of keys) await backend.putObject(key, stream(key), { httpMetadata: {} });
    const first = await backend.listObjects({ prefix: "unicode/", limit: 1 });
    expect(first.truncated).toBe(true);
    if (!first.truncated) return;
    const second = await backend.listObjects({
      prefix: "unicode/",
      limit: 10,
      cursor: first.cursor,
    });
    expect([...first.objects, ...second.objects].map(({ key }) => key)).toEqual(keys);
  });

  it("rejects invalid cursors and handles limit zero", async () => {
    await expect(backend.listObjects({ prefix: "", limit: 10, cursor: "%%%" })).rejects.toThrow(
      "Invalid cursor",
    );
    await expect(backend.listObjects({ prefix: "", limit: 10, cursor: "wA" })).rejects.toThrow(
      "Invalid cursor",
    );
    const result = await backend.listObjects({ prefix: "", limit: 0 });
    expect(result).toEqual({ objects: [], delimitedPrefixes: [], truncated: false });
  });

  it("replaces multipart parts and completes only requested parts", async () => {
    const upload = await backend.createMultipartUpload("multipart", { httpMetadata: {} });
    expect(
      (
        await sql(
          `
            SELECT COUNT(*) AS n
              FROM s3_uploads AS u
         LEFT JOIN s3_objects AS o ON o.upload_id = u.id
             WHERE u.id = ?
               AND o.upload_id IS NULL`,
          upload.uploadId,
        )
      )[0]?.n,
    ).toBe(1);
    const old = await backend.uploadPart("multipart", upload.uploadId, 1, stream("old"));
    const replacement = await backend.uploadPart("multipart", upload.uploadId, 1, stream("new"));
    const extra = await backend.uploadPart("multipart", upload.uploadId, 2, stream("extra"));
    expect(replacement.etag).not.toBe(old.etag);
    await backend.completeMultipartUpload("multipart", upload.uploadId, [replacement]);
    expect(
      (
        await sql(
          `
            SELECT COUNT(*) AS n
              FROM s3_uploads AS u
         LEFT JOIN s3_objects AS o ON o.upload_id = u.id
             WHERE u.id = ?
               AND o.upload_id IS NULL`,
          upload.uploadId,
        )
      )[0]?.n,
    ).toBe(0);
    const result = await backend.getObject("multipart", {});
    if (result.kind !== "found") throw new Error("missing multipart object");
    expect(result.object.size).toBe(3);
    expect(await new Response(result.object.body).text()).toBe("new");
    expect((await sql("SELECT COUNT(*) AS n FROM s3_parts WHERE etag=?", extra.etag))[0]?.n).toBe(
      0,
    );
  });

  it("rolls back a failed multipart replacement and keeps the old part", async () => {
    const upload = await backend.createMultipartUpload("multipart-rollback", { httpMetadata: {} });
    const old = await backend.uploadPart("multipart-rollback", upload.uploadId, 1, stream("old"));
    const failed = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("new-partial"));
        controller.error(new Error("replacement failed"));
      },
    });
    await expect(
      backend.uploadPart("multipart-rollback", upload.uploadId, 1, failed),
    ).rejects.toThrow();
    await backend.completeMultipartUpload("multipart-rollback", upload.uploadId, [old]);
    const result = await backend.getObject("multipart-rollback", {});
    if (result.kind !== "found") throw new Error("missing rolled-back object");
    expect(result.object.size).toBe(3);
    expect(await new Response(result.object.body).text()).toBe("old");
  });

  it("rejects invalid multipart completion and aborts", async () => {
    const upload = await backend.createMultipartUpload("multipart-invalid", { httpMetadata: {} });
    const part = await backend.uploadPart("multipart-invalid", upload.uploadId, 1, stream("part"));
    await expect(
      backend.completeMultipartUpload("wrong", upload.uploadId, [part]),
    ).rejects.toThrow();
    await expect(
      backend.completeMultipartUpload("multipart-invalid", "wrong", [part]),
    ).rejects.toThrow();
    await expect(
      backend.completeMultipartUpload("multipart-invalid", upload.uploadId, []),
    ).rejects.toThrow();
    await expect(
      backend.completeMultipartUpload("multipart-invalid", upload.uploadId, [
        { ...part, etag: "bad" },
      ]),
    ).rejects.toThrow();
    await expect(
      backend.completeMultipartUpload("multipart-invalid", upload.uploadId, [part, part]),
    ).rejects.toThrow();
    await backend.abortMultipartUpload("multipart-invalid", upload.uploadId);
    await expect(
      backend.abortMultipartUpload("multipart-invalid", upload.uploadId),
    ).rejects.toThrow();
  });

  it("does not let a delayed upload mutate a completed or aborted upload", async () => {
    const complete = await backend.createMultipartUpload("multipart-race-complete", {
      httpMetadata: {},
    });
    const published = await backend.uploadPart(
      "multipart-race-complete",
      complete.uploadId,
      1,
      stream("published"),
    );
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const delayedBody = delayedStream(gate, "late");
    const delayed = backend.uploadPart(
      "multipart-race-complete",
      complete.uploadId,
      2,
      delayedBody.stream,
    );
    await delayedBody.started;
    const completing = backend.completeMultipartUpload(
      "multipart-race-complete",
      complete.uploadId,
      [published],
    );
    release();
    await expect(delayed).resolves.toMatchObject({ partNumber: 2 });
    await completing;
    const result = await backend.getObject("multipart-race-complete", {});
    if (result.kind !== "found") throw new Error("missing completed object");
    expect(result.object.size).toBe("published".length);
    expect(await new Response(result.object.body).text()).toBe("published");

    const abort = await backend.createMultipartUpload("multipart-race-abort", { httpMetadata: {} });
    let releaseAbort!: () => void;
    const gateAbort = new Promise<void>((resolve) => (releaseAbort = resolve));
    const delayedAbortBody = delayedStream(gateAbort, "late");
    const delayedAbort = backend.uploadPart(
      "multipart-race-abort",
      abort.uploadId,
      1,
      delayedAbortBody.stream,
    );
    await delayedAbortBody.started;
    const aborting = backend.abortMultipartUpload("multipart-race-abort", abort.uploadId);
    releaseAbort();
    await expect(delayedAbort).resolves.toMatchObject({ partNumber: 1 });
    await aborting;
    expect((await backend.getObject("multipart-race-abort", {})).kind).toBe("not-found");
  });
});

async function sql<T extends Record<string, SqlStorageValue> = { n: number }>(
  query: string,
  ...args: unknown[]
): Promise<T[]> {
  return runInDurableObject(currentStub, (_instance, storageState) =>
    storageState.storage.sql.exec<T>(query, ...args).toArray(),
  );
}

function stream(value: string): ReadableStream<Uint8Array> {
  return new Response(value).body!;
}

function streamBytes(chunks: Uint8Array[]): ReadableStream<Uint8Array> {
  let index = 0;
  return new ReadableStream({
    pull(controller) {
      const chunk = chunks[index++];
      if (chunk) controller.enqueue(chunk);
      else controller.close();
    },
  });
}

function delayedStream(
  gate: Promise<void>,
  value: string,
): {
  stream: ReadableStream<Uint8Array>;
  started: Promise<void>;
} {
  let released = false;
  let startedResolve!: () => void;
  const started = new Promise<void>((resolve) => (startedResolve = resolve));
  const stream = new ReadableStream({
    async pull(controller) {
      startedResolve();
      if (!released) {
        await gate;
        released = true;
      }
      controller.enqueue(new TextEncoder().encode(value));
      controller.close();
    },
  });
  return { stream, started };
}
