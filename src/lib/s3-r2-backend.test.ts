import { env } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { S3ObjectMetadata } from "./s3-backend";
import { R2S3Backend } from "./s3-r2-backend";

const backend = new R2S3Backend(env.GRAFANA_LTX_BUCKET);
const objectPrefix = "test/s3-r2-backend/";

async function cleanupObjects() {
  const objectKeys: string[] = [];
  let cursor: string | undefined;
  do {
    const result = await env.GRAFANA_LTX_BUCKET.list({
      prefix: objectPrefix,
      ...(cursor ? { cursor } : {}),
    });
    objectKeys.push(...result.objects.map(({ key }) => key));
    cursor = result.truncated ? result.cursor : undefined;
  } while (cursor);
  if (objectKeys.length > 0) await env.GRAFANA_LTX_BUCKET.delete(objectKeys);
}

beforeEach(cleanupObjects);
afterEach(async () => {
  await cleanupObjects();
});

function testKey(name: string): string {
  return `${objectPrefix}${name}`;
}

function body(value: string): ReadableStream<Uint8Array> {
  return new Response(value).body!;
}

async function readObject(object: S3ObjectMetadata & { body: ReadableStream<Uint8Array> }) {
  return new Response(object.body).text();
}

describe("R2 S3 backend", () => {
  it("converts metadata fields and preserves raw etags", async () => {
    const key = testKey("metadata");
    const expiry = new Date("2030-01-02T03:04:05Z");
    const put = await backend.putObject(key, body("metadata"), {
      httpMetadata: {
        contentType: "text/plain",
        contentLanguage: "ja",
        contentDisposition: "attachment",
        contentEncoding: "identity",
        cacheControl: "max-age=60",
        cacheExpiry: expiry,
      },
    });
    expect(put.etag).not.toMatch(/^"|"$/);
    expect(put.httpMetadata).toEqual({
      contentType: "text/plain",
      contentLanguage: "ja",
      contentDisposition: "attachment",
      contentEncoding: "identity",
      cacheControl: "max-age=60",
      cacheExpiry: expiry,
    });

    const result = await backend.getObject(key, {});
    expect(result.kind).toBe("found");
    if (result.kind !== "found") return;
    expect(result.object.etag).toBe(put.etag);
    expect(result.object.httpMetadata).toEqual(put.httpMetadata);
    expect(await readObject(result.object)).toBe("metadata");
  });

  it("converts ranges and conditional bodyless results", async () => {
    const key = testKey("range");
    const put = await backend.putObject(key, body("0123456789"), { httpMetadata: {} });
    const ranged = await backend.getObject(key, {
      range: { kind: "offset", offset: 2, length: 4 },
    });
    expect(ranged.kind).toBe("found");
    if (ranged.kind === "found") {
      expect(ranged.object.range).toEqual({ offset: 2, length: 4 });
      expect(await readObject(ranged.object)).toBe("2345");
    }

    const notModified = await backend.getObject(key, {
      conditions: { ifNoneMatch: put.etag },
    });
    expect(notModified.kind).toBe("not-modified");
    const precondition = await backend.getObject(key, {
      conditions: { ifMatch: "does-not-match" },
    });
    expect(precondition.kind).toBe("precondition-failed");
  });

  it("converts list delimiter and cursor values", async () => {
    const objects = [testKey("list/a/one"), testKey("list/b/one"), testKey("list/root")];
    for (const key of objects) {
      await backend.putObject(key, body(key), { httpMetadata: {} });
    }
    const first = await backend.listObjects({
      prefix: testKey("list/"),
      delimiter: "/",
      limit: 1,
    });
    expect(first.delimitedPrefixes).toEqual([testKey("list/a/")]);
    expect(first.truncated).toBe(true);
    if (!first.truncated) return;
    expect(first.cursor).toBeTruthy();
    const second = await backend.listObjects({
      prefix: testKey("list/"),
      delimiter: "/",
      limit: 1,
      cursor: first.cursor,
    });
    expect(second.delimitedPrefixes).toEqual([testKey("list/b/")]);
  });

  it("puts and deletes objects through the backend", async () => {
    const key = testKey("delete");
    await backend.putObject(key, body("delete me"), { httpMetadata: {} });
    await backend.deleteObjects([key]);
    expect(await backend.getObject(key, {})).toEqual({ kind: "not-found" });
  });

  it("completes and aborts multipart uploads", async () => {
    const key = testKey("multipart");
    const created = await backend.createMultipartUpload(key, { httpMetadata: {} });
    const first = await backend.uploadPart(
      key,
      created.uploadId,
      1,
      body("a".repeat(5 * 1024 * 1024)),
    );
    const second = await backend.uploadPart(key, created.uploadId, 2, body("last"));
    const completed = await backend.completeMultipartUpload(key, created.uploadId, [first, second]);
    expect(completed.etag).not.toMatch(/^"|"$/);
    const result = await backend.getObject(key, {});
    expect(result.kind).toBe("found");
    if (result.kind === "found")
      expect(await readObject(result.object)).toHaveLength(5 * 1024 * 1024 + 4);

    const abortedKey = testKey("aborted");
    const aborted = await backend.createMultipartUpload(abortedKey, {
      httpMetadata: {},
    });
    await backend.abortMultipartUpload(abortedKey, aborted.uploadId);
  });
});
