import { env } from "cloudflare:test";
import { AwsClient } from "aws4fetch";
import { XMLParser } from "fast-xml-parser";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createS3App } from "./s3";
import { R2S3Backend } from "./s3-r2-backend";
import type { S3Backend } from "./s3-backend";

const objectPrefix = "test/s3-bridge/";
const bucketName = "grafana-ltx";
const app = createS3App(bucketName, new R2S3Backend(env.GRAFANA_LTX_BUCKET));
const parse = (body: string) =>
  new XMLParser({ htmlEntities: true, removeNSPrefix: true }).parse(body) as Record<
    string,
    unknown
  >;

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

async function cleanup() {
  await cleanupObjects();
}

beforeEach(cleanup);
afterEach(cleanup);

const aws = new AwsClient({
  accessKeyId: "test",
  secretAccessKey: "test",
  region: "us-east-1",
  service: "s3",
});

function xmlCode(body: string): unknown {
  const error = parse(body).Error;
  return typeof error === "object" && error !== null
    ? (error as Record<string, unknown>).Code
    : undefined;
}

async function signedRequest(path: string, init?: RequestInit): Promise<Request> {
  return aws.sign(new Request(`https://grafana-ltx.replica.worker${path}`, init));
}

async function s3Fetch(path: string, init?: RequestInit): Promise<Response> {
  return app.fetch(await signedRequest(`/${bucketName}${path}`, init));
}

async function s3FetchWith(
  backend: S3Backend,
  path: string,
  init?: RequestInit,
): Promise<Response> {
  return createS3App(bucketName, backend).fetch(await signedRequest(`/${bucketName}${path}`, init));
}

function objectPath(key: string): string {
  return `/${key.split("/").map(encodeURIComponent).join("/")}`;
}

describe("S3 bridge", () => {
  it("treats the leading path segment as part of the key", async () => {
    const calls: { get?: unknown; put?: unknown; list?: unknown } = {};
    const metadata = {
      key: "other-bucket/encoded key",
      size: 4,
      etag: "etag",
      uploaded: new Date("2020-01-01T00:00:00Z"),
      httpMetadata: {},
    };
    const fake: S3Backend = {
      getObject: async (key, options) => {
        calls.get = { key, options };
        return { kind: "not-found" };
      },
      putObject: async (key, body, options) => {
        calls.put = { key, body, options };
        return metadata;
      },
      deleteObjects: async () => {},
      listObjects: async (options) => {
        calls.list = options;
        return { objects: [], delimitedPrefixes: [], truncated: false };
      },
      createMultipartUpload: async () => {
        throw new Error("unused");
      },
      uploadPart: async () => {
        throw new Error("unused");
      },
      completeMultipartUpload: async () => {
        throw new Error("unused");
      },
      abortMultipartUpload: async () => {
        throw new Error("unused");
      },
    };

    expect((await s3FetchWith(fake, "/", { method: "HEAD" })).status).toBe(200);
    const put = await s3FetchWith(fake, "/other-bucket/encoded%20key", {
      method: "PUT",
      headers: { "Content-Type": "text/plain" },
      body: "body",
    });
    expect(put.status).toBe(200);
    expect(calls.put).toMatchObject({
      key: "other-bucket/encoded key",
      options: { httpMetadata: { contentType: "text/plain" } },
    });
    const list = await s3FetchWith(fake, "/?list-type=2&prefix=other-bucket%2F", {
      method: "GET",
    });
    expect(list.status).toBe(200);
    expect(calls.list).toEqual({ prefix: "other-bucket/", limit: 1000 });
    await s3FetchWith(fake, "/other-bucket/encoded%20key");
    expect(calls.get).toEqual({ key: "other-bucket/encoded key", options: {} });

    const wrongBucket = await createS3App(bucketName, fake).fetch(
      await signedRequest("/wrong-bucket/encoded%20key"),
    );
    expect(wrongBucket.status).toBe(404);
    expect(xmlCode(await wrongBucket.text())).toBe("NoSuchBucket");
    expect(calls.get).toEqual({ key: "other-bucket/encoded key", options: {} });
  });

  it("passes typed range, conditions, and metadata across the backend boundary", async () => {
    const calls: { get?: unknown; put?: unknown } = {};
    const metadata = {
      key: "boundary/key",
      size: 2,
      etag: "etag",
      uploaded: new Date("2020-01-01T00:00:00Z"),
      httpMetadata: {},
    };
    const fake: S3Backend = {
      getObject: async (key, options) => {
        calls.get = { key, options };
        return {
          kind: "found",
          object: { ...metadata, body: new ReadableStream<Uint8Array>() },
        };
      },
      putObject: async (key, body, options) => {
        calls.put = { key, body, options };
        return metadata;
      },
      deleteObjects: async () => {},
      listObjects: async () => ({ objects: [], delimitedPrefixes: [], truncated: false }),
      createMultipartUpload: async () => {
        throw new Error("unused");
      },
      uploadPart: async () => {
        throw new Error("unused");
      },
      completeMultipartUpload: async () => {
        throw new Error("unused");
      },
      abortMultipartUpload: async () => {
        throw new Error("unused");
      },
    };

    const get = await s3FetchWith(fake, "/other-bucket/key", {
      headers: {
        Range: "bytes=2-5",
        "If-None-Match": '"etag"',
        "If-Modified-Since": "Wed, 01 Jan 2020 00:00:00 GMT",
      },
    });
    expect(get.status).toBe(206);
    expect(calls.get).toEqual({
      key: "other-bucket/key",
      options: {
        range: { kind: "offset", offset: 2, length: 4 },
        conditions: {
          ifNoneMatch: '"etag"',
          ifModifiedSince: new Date("2020-01-01T00:00:00Z"),
        },
      },
    });

    await s3FetchWith(fake, "/other-bucket/put", {
      method: "PUT",
      headers: {
        "Content-Type": "text/plain",
        "Cache-Control": "max-age=60",
        Expires: "invalid",
      },
      body: "body",
    });
    expect(calls.put).toMatchObject({
      key: "other-bucket/put",
      options: {
        httpMetadata: {
          contentType: "text/plain",
          cacheControl: "max-age=60",
        },
      },
    });
  });

  it("implements dummy bucket APIs and keeps trailing-slash bucket routes ahead of objects", async () => {
    const signed = await signedRequest("/");
    expect(signed.headers.get("Authorization")).toMatch(/^AWS4-HMAC-SHA256 /);
    expect(signed.headers.get("x-amz-date")).toMatch(/^\d{8}T\d{6}Z$/);
    expect(signed.headers.get("x-amz-content-sha256")).toMatch(
      /^(?:[\da-f]{64}|UNSIGNED-PAYLOAD)$/,
    );
    const buckets = parse(await (await app.fetch(await signedRequest("/"))).text())
      .ListAllMyBucketsResult as Record<string, unknown>;
    const bucket = (buckets.Buckets as Record<string, unknown>).Bucket as Record<string, unknown>;
    expect(bucket.Name).toBe(bucketName);
    expect((await s3Fetch("/", { method: "HEAD" })).status).toBe(200);
    expect((await s3Fetch("/", { method: "PUT" })).status).toBe(200);
    expect((await s3Fetch("/", { method: "DELETE" })).status).toBe(204);
  });

  it("puts, gets, heads, and deletes objects", async () => {
    const key = "test/s3-bridge/basic.txt";
    const put = await s3Fetch(objectPath(key), {
      method: "PUT",
      headers: { "Content-Type": "text/plain" },
      body: "hello",
    });
    expect(put.status).toBe(200);
    expect(put.headers.get("ETag")).toMatch(/^".+"$/);
    const head = await s3Fetch(objectPath(key), { method: "HEAD" });
    expect(head.headers.get("Content-Type")).toBe("text/plain");
    expect(head.headers.get("Content-Length")).toBe("5");
    const get = await s3Fetch(objectPath(key));
    expect(await get.text()).toBe("hello");
    expect((await s3Fetch(objectPath(key), { method: "DELETE" })).status).toBe(204);
    const missing = await s3Fetch(objectPath(key));
    expect(missing.status).toBe(404);
    expect(xmlCode(await missing.text())).toBe("NoSuchKey");
  });

  it("decodes an encoded key and serves byte ranges", async () => {
    const key = "test/s3-bridge/encoded/name with spaces & symbols.txt";
    await s3Fetch(objectPath(key), { method: "PUT", body: "0123456789" });
    const range = await s3Fetch(objectPath(key), { headers: { Range: "bytes=2-5" } });
    expect(await range.text()).toBe("2345");
    expect(range.headers.get("Content-Length")).toBe("4");
    expect(range.headers.get("Content-Range")).toBe("bytes 2-5/10");
    const head = await s3Fetch(objectPath(key), {
      method: "HEAD",
      headers: { Range: "bytes=2-5" },
    });
    expect(head.headers.get("Content-Length")).toBe("4");
    const openEnded = await s3Fetch(objectPath(key), { headers: { Range: "bytes=7-" } });
    expect(await openEnded.text()).toBe("789");
    const suffix = await s3Fetch(objectPath(key), { headers: { Range: "bytes=-3" } });
    expect(await suffix.text()).toBe("789");
  });

  it("rejects invalid and multiple byte ranges", async () => {
    const key = "test/s3-bridge/invalid-range";
    await s3Fetch(objectPath(key), { method: "PUT", body: "0123456789" });
    for (const range of ["bytes=5-2", "bytes=1-2,4-5"]) {
      const response = await s3Fetch(objectPath(key), { headers: { Range: range } });
      expect(response.status).toBe(416);
      expect(xmlCode(await response.text())).toBe("InvalidRange");
    }
    const rangeBackend: S3Backend = {
      getObject: async () => ({ kind: "range-not-satisfiable", size: 10 }),
      putObject: async () => {
        throw new Error("unused");
      },
      deleteObjects: async () => {},
      listObjects: async () => ({ objects: [], delimitedPrefixes: [], truncated: false }),
      createMultipartUpload: async () => {
        throw new Error("unused");
      },
      uploadPart: async () => {
        throw new Error("unused");
      },
      completeMultipartUpload: async () => {
        throw new Error("unused");
      },
      abortMultipartUpload: async () => {
        throw new Error("unused");
      },
    };
    const outside = await s3FetchWith(rangeBackend, "/range", {
      headers: { Range: "bytes=99-" },
    });
    expect(outside.status).toBe(416);
    expect(outside.headers.get("Content-Range")).toBe("bytes */10");
  });

  it("escapes special characters in XML object keys", async () => {
    const key = `test/s3-bridge/xml/&<>"'`;
    await s3Fetch(objectPath(key), { method: "PUT", body: "value" });
    const response = await s3Fetch(
      `/?list-type=2&prefix=${encodeURIComponent("test/s3-bridge/xml/")}`,
    );
    const body = await response.text();
    expect(body).toContain("&amp;&lt;&gt;&quot;&#39;");
    const result = parse(body).ListBucketResult as Record<string, unknown>;
    const contents = Array.isArray(result.Contents) ? result.Contents : [result.Contents];
    expect((contents[0] as Record<string, unknown>).Key).toBe(key);
  });

  it("lists v2 objects with prefix and delimiter", async () => {
    const objects = [
      "test/s3-bridge/tree/a/one",
      "test/s3-bridge/tree/a/two",
      "test/s3-bridge/tree/b/one",
      "test/s3-bridge/unrelated",
    ];
    for (const key of objects) {
      await s3Fetch(objectPath(key), { method: "PUT", body: key });
    }
    const response = parse(
      await (await s3Fetch("/?list-type=2&prefix=test%2Fs3-bridge%2Ftree%2F&delimiter=%2F")).text(),
    ).ListBucketResult as Record<string, unknown>;
    expect(response.KeyCount).toBe(2);
    const commonPrefixes = response.CommonPrefixes as Array<Record<string, unknown>>;
    expect(commonPrefixes.map(({ Prefix }) => Prefix)).toEqual([
      "test/s3-bridge/tree/a/",
      "test/s3-bridge/tree/b/",
    ]);
  });

  it("applies v2 start-after and v1 marker, including v1 NextMarker", async () => {
    const objects = ["test/s3-bridge/order-a", "test/s3-bridge/order-b", "test/s3-bridge/order-c"];
    for (const key of objects) {
      await s3Fetch(objectPath(key), { method: "PUT", body: key });
    }
    const v2 = parse(
      await (
        await s3Fetch(
          "/?list-type=2&prefix=test%2Fs3-bridge%2Forder-&start-after=test%2Fs3-bridge%2Forder-a",
        )
      ).text(),
    ).ListBucketResult as Record<string, unknown>;
    const v2Contents = v2.Contents as Array<Record<string, unknown>>;
    expect(v2Contents.map(({ Key }) => Key)).toEqual([
      "test/s3-bridge/order-b",
      "test/s3-bridge/order-c",
    ]);
    const v1 = parse(
      await (
        await s3Fetch(
          "/?prefix=test%2Fs3-bridge%2Forder-&marker=test%2Fs3-bridge%2Forder-a&max-keys=1",
        )
      ).text(),
    ).ListBucketResult as Record<string, unknown>;
    const v1Contents = Array.isArray(v1.Contents)
      ? (v1.Contents as Array<Record<string, unknown>>)
      : [v1.Contents as Record<string, unknown>];
    expect(v1Contents[0]?.Key).toBe("test/s3-bridge/order-b");
    expect(v1.NextMarker).toBe("test/s3-bridge/order-b");
  });

  it("returns a usable v1 NextMarker for a common-prefix-only page", async () => {
    const group = "test/s3-bridge/marker/group/";
    let marker: string | undefined;
    const markerBackend: S3Backend = {
      getObject: async () => ({ kind: "not-found" }),
      putObject: async () => {
        throw new Error("unused");
      },
      deleteObjects: async () => {},
      listObjects: async (options) => {
        marker = options.startAfter;
        return options.startAfter === undefined
          ? { objects: [], delimitedPrefixes: [group], truncated: true, cursor: "cursor" }
          : {
              objects: [
                {
                  key: "test/s3-bridge/marker/root",
                  size: 4,
                  etag: "etag",
                  uploaded: new Date(0),
                  httpMetadata: {},
                },
              ],
              delimitedPrefixes: [],
              truncated: false,
            };
      },
      createMultipartUpload: async () => {
        throw new Error("unused");
      },
      uploadPart: async () => {
        throw new Error("unused");
      },
      completeMultipartUpload: async () => {
        throw new Error("unused");
      },
      abortMultipartUpload: async () => {
        throw new Error("unused");
      },
    };
    const first = parse(
      await (
        await s3FetchWith(
          markerBackend,
          "/?prefix=test%2Fs3-bridge%2Fmarker%2F&delimiter=%2F&max-keys=1",
        )
      ).text(),
    ).ListBucketResult as Record<string, unknown>;
    expect(first.NextMarker).toBe("test/s3-bridge/marker/group/");
    const second = parse(
      await (
        await s3FetchWith(
          markerBackend,
          `/?prefix=test%2Fs3-bridge%2Fmarker%2F&delimiter=%2F&max-keys=1&marker=${encodeURIComponent(String(first.NextMarker))}`,
        )
      ).text(),
    ).ListBucketResult as Record<string, unknown>;
    expect(second.Contents).toBeDefined();
    expect(second.CommonPrefixes).toBeUndefined();
    expect(marker).toBe(group);
  });

  it("deletes objects in a batch", async () => {
    const objects = ["test/s3-bridge/delete-one", "test/s3-bridge/delete-two"];
    for (const key of objects) {
      await s3Fetch(objectPath(key), { method: "PUT", body: key });
    }
    const response = await s3Fetch("/?delete", {
      method: "POST",
      headers: { "Content-Type": "application/xml" },
      body: `<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">${objects.map((Key) => `<Object><Key>${Key}</Key></Object>`).join("")}</Delete>`,
    });
    expect(response.status).toBe(200);
    const deleted = parse(await response.text()).DeleteResult as Record<string, unknown>;
    expect((deleted.Deleted as Array<Record<string, unknown>>).map(({ Key }) => Key)).toEqual(
      objects,
    );
    for (const key of objects) {
      expect(await env.GRAFANA_LTX_BUCKET.head(key)).toBeNull();
    }
  });

  it("rejects a syntactically valid DeleteObjects document with the wrong root", async () => {
    const response = await s3Fetch("/?delete", {
      method: "POST",
      headers: { "Content-Type": "application/xml" },
      body: "<WrongRoot><Object><Key>ignored</Key></Object></WrongRoot>",
    });
    expect(response.status).toBe(400);
    expect(xmlCode(await response.text())).toBe("InvalidRequest");
  });

  it("rejects a DeleteObjects document with mismatched closing tags", async () => {
    const response = await s3Fetch("/?delete", {
      method: "POST",
      headers: { "Content-Type": "application/xml" },
      body: "<Delete><Object><Key>x</Delete>",
    });
    expect(response.status).toBe(400);
    expect(xmlCode(await response.text())).toBe("MalformedXML");
  });

  it("completes a multipart upload", async () => {
    const key = "test/s3-bridge/multipart.txt";
    const initiate = await s3Fetch(`${objectPath(key)}?uploads`, { method: "POST" });
    const initiateResult = parse(await initiate.text()).InitiateMultipartUploadResult as Record<
      string,
      unknown
    >;
    const uploadId = initiateResult.UploadId;
    expect(uploadId).toBeTruthy();
    if (typeof uploadId !== "string") throw new Error("Missing multipart upload ID");
    const firstBody = "a".repeat(5 * 1024 * 1024);
    const secondBody = "multipart";
    const partResponses = [];
    for (const [index, body] of [firstBody, secondBody].entries()) {
      partResponses.push(
        await s3Fetch(
          `${objectPath(key)}?partNumber=${index + 1}&uploadId=${encodeURIComponent(uploadId)}`,
          { method: "PUT", body },
        ),
      );
    }
    const etags = partResponses.map((part) => part.headers.get("ETag"));
    expect(etags.every((etag) => etag)).toBe(true);
    const complete = await s3Fetch(`${objectPath(key)}?uploadId=${encodeURIComponent(uploadId)}`, {
      method: "POST",
      headers: { "Content-Type": "application/xml" },
      body: `<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">${etags.map((ETag, index) => `<Part><ETag>${ETag?.replaceAll('"', "&#34;")}</ETag><PartNumber>${index + 1}</PartNumber></Part>`).join("")}</CompleteMultipartUpload>`,
    });
    expect(
      (parse(await complete.text()).CompleteMultipartUploadResult as Record<string, unknown>).Key,
    ).toBe(key);
    const objectResponse = await s3Fetch(objectPath(key));
    const object = await objectResponse.text();
    expect(object.length).toBe(firstBody.length + secondBody.length);
    expect(object.startsWith(firstBody)).toBe(true);
    expect(object.endsWith(secondBody)).toBe(true);
  });

  it("aborts a multipart upload through the signed S3 client", async () => {
    const key = "test/s3-bridge/multipart-abort.txt";
    const initiate = await s3Fetch(`${objectPath(key)}?uploads`, { method: "POST" });
    const uploadId = (
      parse(await initiate.text()).InitiateMultipartUploadResult as Record<string, unknown>
    ).UploadId;
    expect(uploadId).toBeTruthy();
    if (typeof uploadId !== "string") throw new Error("Missing multipart upload ID");
    expect(
      (
        await s3Fetch(`${objectPath(key)}?uploadId=${encodeURIComponent(uploadId)}`, {
          method: "DELETE",
        })
      ).status,
    ).toBe(204);
  });

  it("returns S3 errors for malformed or invalid multipart completion", async () => {
    const key = "test/s3-bridge/multipart-invalid.txt";
    const initiate = await s3Fetch(`${objectPath(key)}?uploads`, { method: "POST" });
    const uploadId = (
      parse(await initiate.text()).InitiateMultipartUploadResult as Record<string, unknown>
    ).UploadId;
    expect(uploadId).toBeTruthy();
    if (typeof uploadId !== "string") throw new Error("Missing multipart upload ID");
    const wrongRoot = await s3Fetch(`${objectPath(key)}?uploadId=${encodeURIComponent(uploadId)}`, {
      method: "POST",
      body: "<WrongRoot><Part><PartNumber>1</PartNumber><ETag>etag</ETag></Part></WrongRoot>",
    });
    expect(wrongRoot.status).toBe(400);
    expect(xmlCode(await wrongRoot.text())).toBe("InvalidPart");

    const malformed = await s3Fetch(`${objectPath(key)}?uploadId=${encodeURIComponent(uploadId)}`, {
      method: "POST",
      body: "<CompleteMultipartUpload>",
    });
    expect(malformed.status).toBe(400);
    expect(xmlCode(await malformed.text())).toBe("MalformedXML");

    const invalid = await s3Fetch(`${objectPath(key)}?uploadId=${encodeURIComponent(uploadId)}`, {
      method: "POST",
      body: "<CompleteMultipartUpload><Part><PartNumber>nope</PartNumber><ETag>bad</ETag></Part></CompleteMultipartUpload>",
    });
    expect(invalid.status).toBe(400);
    expect(xmlCode(await invalid.text())).toBe("InvalidPart");

    const missing = await s3Fetch(`${objectPath(key)}?uploadId=missing`, { method: "DELETE" });
    expect(missing.status).toBe(404);
    expect(xmlCode(await missing.text())).toBe("NoSuchUpload");
  });
});
