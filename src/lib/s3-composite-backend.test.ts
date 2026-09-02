import { describe, expect, it } from "vitest";
import type {
  S3Backend,
  S3GetOptions,
  S3GetResult,
  S3ListOptions,
  S3ListResult,
  S3MultipartPart,
  S3MultipartUpload,
  S3ObjectMetadata,
  S3PutOptions,
} from "./s3-backend";
import { CompositeS3Backend, type S3BackendRoute } from "./s3-composite-backend";

function compareUtf8(left: string, right: string): number {
  const leftBytes = new TextEncoder().encode(left);
  const rightBytes = new TextEncoder().encode(right);
  const length = Math.min(leftBytes.length, rightBytes.length);
  for (let index = 0; index < length; index++) {
    const leftByte = leftBytes[index];
    const rightByte = rightBytes[index];
    if (leftByte === undefined || rightByte === undefined) continue;
    if (leftByte < rightByte) return -1;
    if (leftByte > rightByte) return 1;
  }
  return leftBytes.length - rightBytes.length;
}

class MemoryBackend implements S3Backend {
  readonly objects: S3ObjectMetadata[];
  readonly calls: { readonly method: string; readonly key: string; readonly uploadId?: string }[] =
    [];
  readonly listCalls: S3ListOptions[] = [];

  constructor(keys: readonly string[] = []) {
    this.objects = keys.map((key) => metadata(key));
  }

  async getObject(key: string, _options: S3GetOptions): Promise<S3GetResult> {
    this.calls.push({ method: "get", key });
    const object = this.objects.find((candidate) => candidate.key === key);
    return object === undefined
      ? { kind: "not-found" }
      : { kind: "found", object: { ...object, body: new ReadableStream() } };
  }

  async putObject(key: string, _body: ReadableStream<Uint8Array> | null, _options: S3PutOptions) {
    this.calls.push({ method: "put", key });
    return metadata(key);
  }

  async deleteObjects(keys: readonly string[]) {
    for (const key of keys) this.calls.push({ method: "delete", key });
  }

  async listObjects(options: S3ListOptions): Promise<S3ListResult> {
    this.listCalls.push(options);
    const matching = this.objects
      .filter((object) => object.key.startsWith(options.prefix))
      .sort((left, right) => compareUtf8(left.key, right.key));
    let index = options.cursor === undefined ? 0 : Number(options.cursor);
    if (options.cursor === undefined && options.startAfter !== undefined) {
      index = matching.findIndex((object) => compareUtf8(object.key, options.startAfter!) > 0);
      if (index < 0) index = matching.length;
    }
    const object = matching[index];
    if (object === undefined) return { objects: [], delimitedPrefixes: [], truncated: false };
    return index + 1 < matching.length
      ? { objects: [object], delimitedPrefixes: [], truncated: true, cursor: String(index + 1) }
      : { objects: [object], delimitedPrefixes: [], truncated: false };
  }

  async createMultipartUpload(key: string, _options: S3PutOptions): Promise<S3MultipartUpload> {
    this.calls.push({ method: "create", key });
    return { uploadId: `upload:${key}` };
  }

  async uploadPart(
    key: string,
    uploadId: string,
    _partNumber: number,
    _body: ReadableStream<Uint8Array> | null,
  ) {
    this.calls.push({ method: "part", key, uploadId });
    return { partNumber: 1, etag: "etag" };
  }

  async completeMultipartUpload(key: string, uploadId: string, _parts: readonly S3MultipartPart[]) {
    this.calls.push({ method: "complete", key, uploadId });
    return metadata(key);
  }

  async abortMultipartUpload(key: string, uploadId: string) {
    this.calls.push({ method: "abort", key, uploadId });
  }
}

function metadata(key: string): S3ObjectMetadata {
  return {
    key,
    size: key.length,
    etag: `etag:${key}`,
    uploaded: new Date(0),
    httpMetadata: {},
  };
}

function composite(routes: readonly S3BackendRoute[] = [], defaultBackend?: S3Backend) {
  return new CompositeS3Backend(routes, defaultBackend);
}

describe("CompositeS3Backend", () => {
  it("validates and sorts route prefixes", () => {
    const first = new MemoryBackend();
    const second = new MemoryBackend();
    expect(() => composite([{ prefix: "", backend: first }])).toThrow();
    expect(() => composite([{ prefix: "\u0000default", backend: first }])).toThrow();
    expect(() =>
      composite([
        { prefix: "a", backend: first },
        { prefix: "a", backend: second },
      ]),
    ).toThrow();
    expect(() =>
      composite([
        { prefix: "a", backend: first },
        { prefix: "ab", backend: second },
      ]),
    ).toThrow();
  });

  it("routes full keys, prefers explicit routes, and uses the default", async () => {
    const explicit = new MemoryBackend(["logs/item"]);
    const fallback = new MemoryBackend(["other"]);
    const backend = composite([{ prefix: "logs/", backend: explicit }], fallback);
    expect((await backend.getObject("logs/item", {})).kind).toBe("found");
    expect((await backend.getObject("other", {})).kind).toBe("found");
    expect((await backend.getObject("missing", {})).kind).toBe("not-found");
    expect(explicit.calls).toEqual([{ method: "get", key: "logs/item" }]);
    expect(fallback.calls).toEqual([
      { method: "get", key: "other" },
      { method: "get", key: "missing" },
    ]);
  });

  it("returns not-found and no-ops deletes without a default", async () => {
    const route = new MemoryBackend();
    const backend = composite([{ prefix: "route/", backend: route }]);
    expect((await backend.getObject("other", {})).kind).toBe("not-found");
    await backend.deleteObjects(["other"]);
    expect(route.calls).toEqual([]);
    await expect(backend.putObject("other", null, { httpMetadata: {} })).rejects.toThrow();
  });

  it("partitions deletes by backend and calls each backend once", async () => {
    const left = new MemoryBackend();
    const right = new MemoryBackend();
    const backend = composite(
      [
        { prefix: "a/", backend: left },
        { prefix: "b/", backend: right },
      ],
      left,
    );
    await backend.deleteObjects(["a/1", "b/1", "a/2", "fallback"]);
    expect(left.calls).toEqual([
      { method: "delete", key: "a/1" },
      { method: "delete", key: "a/2" },
      { method: "delete", key: "fallback" },
    ]);
    expect(right.calls).toEqual([{ method: "delete", key: "b/1" }]);
  });

  it("routes all multipart operations and preserves upload ids", async () => {
    const route = new MemoryBackend();
    const backend = composite([{ prefix: "parts/", backend: route }]);
    await backend.createMultipartUpload("parts/file", { httpMetadata: {} });
    await backend.uploadPart("parts/file", "id", 1, null);
    await backend.completeMultipartUpload("parts/file", "id", [{ partNumber: 1, etag: "e" }]);
    await backend.abortMultipartUpload("parts/file", "id");
    expect(route.calls.map(({ method, key, uploadId }) => ({ method, key, uploadId }))).toEqual([
      { method: "create", key: "parts/file", uploadId: undefined },
      { method: "part", key: "parts/file", uploadId: "id" },
      { method: "complete", key: "parts/file", uploadId: "id" },
      { method: "abort", key: "parts/file", uploadId: "id" },
    ]);
  });

  it("merges all sources in lexical order and shadows explicit prefixes in default", async () => {
    const route = new MemoryBackend(["a/route", "c/route"]);
    const fallback = new MemoryBackend(["a/default", "b/default", "d/default"]);
    const backend = composite(
      [
        { prefix: "a/", backend: route },
        { prefix: "c/", backend: route },
      ],
      fallback,
    );
    const objects: string[] = [];
    let cursor: string | undefined;
    do {
      const page = await backend.listObjects({
        prefix: "",
        limit: 1,
        ...(cursor ? { cursor } : {}),
      });
      objects.push(...page.objects.map(({ key }) => key));
      cursor = page.truncated ? page.cursor : undefined;
    } while (cursor !== undefined);
    expect(objects).toEqual(["a/route", "b/default", "c/route", "d/default"]);
  });

  it("does not query the default backend below an explicit route", async () => {
    const explicit = new MemoryBackend(["a/route/item"]);
    const fallback = new MemoryBackend(["a/route/shadowed", "other"]);
    const backend = composite([{ prefix: "a/route/", backend: explicit }], fallback);

    const page = await backend.listObjects({ prefix: "a/route/", limit: 10 });

    expect(page.objects.map(({ key }) => key)).toEqual(["a/route/item"]);
    expect(explicit.listCalls).toHaveLength(1);
    expect(fallback.listCalls).toEqual([]);
  });

  it("groups shared delimiter prefixes across sources", async () => {
    const first = new MemoryBackend(["a/x/1", "a/x/2"]);
    const second = new MemoryBackend(["a/y/1"]);
    const fallback = new MemoryBackend(["a/default", "z/item"]);
    const backend = composite(
      [
        { prefix: "a/x/", backend: first },
        { prefix: "a/y/", backend: second },
      ],
      fallback,
    );
    const page = await backend.listObjects({ prefix: "", delimiter: "/", limit: 10 });
    expect(page.objects.map(({ key }) => key)).toEqual([]);
    expect(page.delimitedPrefixes).toEqual(["a/", "z/"]);
  });

  it("supports empty sources, startAfter, and strict request-bound cursors", async () => {
    const route = new MemoryBackend(["a", "aa"]);
    const backend = composite([{ prefix: "a", backend: route }]);
    const page = await backend.listObjects({ prefix: "a", limit: 1, startAfter: "a" });
    expect(page.objects.map(({ key }) => key)).toEqual(["aa"]);
    expect(route.listCalls[0]?.startAfter).toBe("a");
    expect((await composite().listObjects({ prefix: "x", limit: 1 })).truncated).toBe(false);
    const paged = await backend.listObjects({ prefix: "a", limit: 1 });
    if (!paged.truncated) throw new Error("expected cursor");
    await expect(
      backend.listObjects({ prefix: "b", limit: 1, cursor: paged.cursor }),
    ).rejects.toThrow("Invalid cursor");
    await expect(
      backend.listObjects({ prefix: "a", limit: 1, cursor: "not-base64" }),
    ).rejects.toThrow("Invalid cursor");
    const unknownTopLevel = rewriteCursor(paged.cursor, (value) => {
      value.extra = true;
    });
    await expect(
      backend.listObjects({ prefix: "a", limit: 1, cursor: unknownTopLevel }),
    ).rejects.toThrow("Invalid cursor");
    const unknownSource = rewriteCursor(paged.cursor, (value) => {
      const source = value.sources[0];
      if (source === undefined) throw new Error("missing source");
      source.extra = true;
    });
    await expect(
      backend.listObjects({ prefix: "a", limit: 1, cursor: unknownSource }),
    ).rejects.toThrow("Invalid cursor");
  });

  it("reuses startAfter when an unconsumed head is refetched after a cursor", async () => {
    const explicit = new MemoryBackend(["b/explicit"]);
    const fallback = new MemoryBackend(["a0", "z-after"]);
    const backend = composite([{ prefix: "b/", backend: explicit }], fallback);
    const first = await backend.listObjects({ prefix: "", limit: 1, startAfter: "a0" });
    expect(first.objects.map(({ key }) => key)).toEqual(["b/explicit"]);
    if (!first.truncated) throw new Error("expected cursor");

    const second = await backend.listObjects({
      prefix: "",
      limit: 10,
      cursor: first.cursor,
      startAfter: "ignored-on-continuation",
    });
    expect(second.objects.map(({ key }) => key)).toEqual(["z-after"]);
    expect(second.objects.map(({ key }) => key)).not.toContain("a0");
  });

  it("does not duplicate or skip objects over pages and ends untruncated", async () => {
    const first = new MemoryBackend(["a/x/1", "a/x/3"]);
    const second = new MemoryBackend(["a/y/2", "a/y/4"]);
    const backend = composite([{ prefix: "a/x/", backend: first }], second);
    const keys: string[] = [];
    let cursor: string | undefined;
    let final: S3ListResult | undefined;
    do {
      final = await backend.listObjects({ prefix: "a/", limit: 1, ...(cursor ? { cursor } : {}) });
      keys.push(...final.objects.map(({ key }) => key));
      cursor = final.truncated ? final.cursor : undefined;
    } while (cursor !== undefined);
    expect(keys).toEqual(["a/x/1", "a/x/3", "a/y/2", "a/y/4"]);
    expect(final?.truncated).toBe(false);
  });

  it("orders non-BMP keys by UTF-8 bytes across cursor pages", async () => {
    const privateUse = new MemoryBackend(["\uE000/object"]);
    const supplementary = new MemoryBackend(["\u{10000}/object"]);
    const backend = composite([
      { prefix: "\uE000/", backend: privateUse },
      { prefix: "\u{10000}/", backend: supplementary },
    ]);
    const keys: string[] = [];
    let cursor: string | undefined;
    do {
      const page = await backend.listObjects({
        prefix: "",
        limit: 1,
        ...(cursor === undefined ? {} : { cursor }),
      });
      keys.push(...page.objects.map(({ key }) => key));
      cursor = page.truncated ? page.cursor : undefined;
    } while (cursor !== undefined);
    expect(keys).toEqual(["\uE000/object", "\u{10000}/object"]);
  });
});

function rewriteCursor(
  value: string,
  rewrite: (value: { readonly sources: Record<string, unknown>[]; [key: string]: unknown }) => void,
): string {
  const parsed = JSON.parse(
    new TextDecoder().decode(Uint8Array.fromBase64(value, { alphabet: "base64url" })),
  ) as {
    readonly sources: Record<string, unknown>[];
    [key: string]: unknown;
  };
  rewrite(parsed);
  return new TextEncoder().encode(JSON.stringify(parsed)).toBase64({
    alphabet: "base64url",
    omitPadding: true,
  });
}
