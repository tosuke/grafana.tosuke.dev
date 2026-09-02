/** @jsxImportSource hono/jsx */
/** @jsxRuntime automatic */

import { SyntaxValidator } from "fast-xml-validator";
import { XMLParser } from "fast-xml-parser";
import { Hono, type Context } from "hono";
import * as z from "zod/mini";
import type {
  S3Backend,
  S3ByteRange,
  S3Conditions,
  S3GetOptions,
  S3HttpMetadata,
  S3MultipartPart,
  S3MultipartUpload,
  S3ObjectMetadata,
} from "./s3-backend";
import { resolveCallbackSync } from "hono/utils/html";
import type { JSX } from "hono/jsx/jsx-runtime";

const XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/";

const ListBucketResult = "ListBucketResult";
const ListAllMyBucketsResult = "ListAllMyBucketsResult";
const Owner = "Owner";
const ID = "ID";
const DisplayName = "DisplayName";
const Buckets = "Buckets";
const CreationDate = "CreationDate";
const Name = "Name";
const Prefix = "Prefix";
const KeyCount = "KeyCount";
const MaxKeys = "MaxKeys";
const Delimiter = "Delimiter";
const StartAfter = "StartAfter";
const Marker = "Marker";
const IsTruncated = "IsTruncated";
const NextContinuationToken = "NextContinuationToken";
const NextMarker = "NextMarker";
const Contents = "Contents";
const LastModified = "LastModified";
const ETag = "ETag";
const Size = "Size";
const StorageClass = "StorageClass";
const CommonPrefixes = "CommonPrefixes";
const DeleteResult = "DeleteResult";
const Deleted = "Deleted";
const InitiateMultipartUploadResult = "InitiateMultipartUploadResult";
const Bucket = "Bucket";
const Key = "Key";
const UploadId = "UploadId";
const CompleteMultipartUploadResult = "CompleteMultipartUploadResult";
const Location = "Location";
const ErrorElement = "Error";
const Code = "Code";
const Message = "Message";
const Resource = "Resource";
const RequestId = "RequestId";

const deleteObjectsRequestSchema = z.object({
  Delete: z.object({
    Object: z.union([z.object({ Key: z.string() }), z.array(z.object({ Key: z.string() }))]),
  }),
});

const completeMultipartUploadRequestSchema = z.object({
  CompleteMultipartUpload: z.object({
    Part: z.union([
      z.object({ PartNumber: z.string(), ETag: z.string() }),
      z.array(z.object({ PartNumber: z.string(), ETag: z.string() })),
    ]),
  }),
});

/**
 * A deliberately small S3 facade for the configured object backend.
 *
 * Authentication is intentionally left to the caller.  In particular, this
 * app accepts (but does not verify) SigV4 headers, which makes it useful for
 * testing S3 clients against a Worker route.
 */
export function createS3App(bucketName: string, backend: S3Backend): Hono {
  const app = new Hono();

  app.get("/", (c) => listBucketsResult(c, bucketName));

  for (const path of ["/:bucket", "/:bucket/"]) {
    app.get(path, (c) => bucketRequest(c, bucketName, () => listObjects(c, backend, bucketName)));
    app.on("HEAD", path, (c) => bucketRequest(c, bucketName, dummyHeadBucket));
    app.put(path, (c) => bucketRequest(c, bucketName, dummyCreateBucket));
    app.delete(path, (c) => bucketRequest(c, bucketName, dummyDeleteBucket));
    app.post(path, (c) =>
      bucketRequest(c, bucketName, () =>
        c.req.query("delete") !== undefined
          ? deleteObjects(c, backend)
          : notImplemented(c.req.path),
      ),
    );
  }

  app.get("/:bucket/*", (c) =>
    objectRequest(c, bucketName, (key) => getObject(c, backend, key, false)),
  );
  app.on("HEAD", "/:bucket/*", (c) =>
    objectRequest(c, bucketName, (key) => getObject(c, backend, key, true)),
  );
  app.put("/:bucket/*", (c) =>
    objectRequest(c, bucketName, (key) => {
      const partNumber = c.req.query("partNumber");
      const uploadId = c.req.query("uploadId");
      const hasPartNumber = partNumber !== undefined;
      const hasUploadId = uploadId !== undefined;
      if (hasPartNumber && hasUploadId) {
        return uploadPart(c, backend, key, partNumber, uploadId);
      }
      if (hasPartNumber || hasUploadId) {
        return s3Error(
          "InvalidRequest",
          "Both partNumber and uploadId are required.",
          c.req.path,
          400,
        );
      }
      return putObject(c, backend, key);
    }),
  );
  app.delete("/:bucket/*", (c) =>
    objectRequest(c, bucketName, (key) => {
      const uploadId = c.req.query("uploadId");
      return uploadId !== undefined
        ? abortMultipart(c, backend, key, uploadId)
        : deleteObject(backend, key);
    }),
  );
  app.post("/:bucket/*", (c) =>
    objectRequest(c, bucketName, (key) => {
      const uploads = c.req.query("uploads");
      const uploadId = c.req.query("uploadId");
      if (uploads !== undefined) return createMultipart(c, backend, bucketName, key);
      if (uploadId !== undefined) {
        return completeMultipart(c, backend, bucketName, key, uploadId);
      }
      return notImplemented(c.req.path);
    }),
  );

  app.all("*", (c) => notImplemented(c.req.path));
  return app;
}

function objectKey(path: string): string {
  const value = path.replace(/^\/[^/]+\/?/, "");
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function bucketRequest(
  c: Context,
  bucketName: string,
  handler: () => Response | Promise<Response>,
): Response | Promise<Response> {
  if (c.req.param("bucket") !== bucketName) {
    return s3Error("NoSuchBucket", "The specified bucket does not exist.", c.req.path, 404);
  }
  return handler();
}

function objectRequest(
  c: Context,
  bucketName: string,
  handler: (key: string) => Response | Promise<Response>,
): Response | Promise<Response> {
  if (c.req.param("bucket") !== bucketName) {
    return s3Error("NoSuchBucket", "The specified bucket does not exist.", c.req.path, 404);
  }
  const key = objectKey(c.req.path);
  if (!key) return s3Error("InvalidRequest", "Object key must not be empty", c.req.path, 400);
  return handler(key);
}

function dummyHeadBucket(): Response {
  return new Response(null, { status: 200 });
}

function dummyCreateBucket(): Response {
  return new Response(null, {
    status: 200,
    headers: { Location: "/" },
  });
}

function dummyDeleteBucket(): Response {
  return new Response(null, { status: 204 });
}

async function deleteObject(backend: S3Backend, key: string): Promise<Response> {
  await backend.deleteObjects([key]);
  return new Response(null, { status: 204 });
}

async function getObject(
  c: Context,
  backend: S3Backend,
  key: string,
  head: boolean,
): Promise<Response> {
  const requestHeaders = c.req.raw.headers;
  const range = parseRange(requestHeaders.get("Range"));
  if (range === "invalid") {
    return s3Error("InvalidRange", "The requested range is not valid.", c.req.path, 416);
  }
  const conditions = requestConditions(requestHeaders);
  const options: S3GetOptions = {
    ...(range ? { range } : {}),
    ...(conditions ? { conditions } : {}),
  };
  const result = await backend.getObject(key, options);
  if (result.kind === "not-found") {
    return s3Error("NoSuchKey", "The specified key does not exist.", c.req.path, 404);
  }
  if (result.kind === "range-not-satisfiable") {
    const response = s3Error("InvalidRange", "The requested range is not valid.", c.req.path, 416);
    response.headers.set("Content-Range", `bytes */${result.size}`);
    return response;
  }
  if (result.kind === "not-modified") {
    return new Response(null, { status: 304, headers: objectHeaders(result.object) });
  }
  if (result.kind === "precondition-failed") {
    return new Response(null, { status: 412, headers: objectHeaders(result.object) });
  }
  const headers = objectHeaders(result.object);
  if (range && result.object.range) {
    const { offset, length } = result.object.range;
    headers.set("Content-Range", `bytes ${offset}-${offset + length - 1}/${result.object.size}`);
    headers.set("Content-Length", String(length));
  }
  const status = range ? 206 : 200;
  return new Response(head ? null : result.object.body, { status, headers });
}

async function putObject(c: Context, backend: S3Backend, key: string): Promise<Response> {
  const object = await backend.putObject(key, c.req.raw.body, {
    httpMetadata: requestHttpMetadata(c.req.raw.headers),
  });
  const headers = new Headers({ ETag: quoteEtag(object.etag) });
  return new Response(null, { status: 200, headers });
}

async function listObjects(c: Context, backend: S3Backend, bucketName: string): Promise<Response> {
  const v2 = c.req.query("list-type") === "2";
  const maxKeys = parseMaxKeys(c.req.query("max-keys"));
  const prefix = c.req.query("prefix") ?? "";
  const delimiter = c.req.query("delimiter") ?? undefined;
  const options: {
    prefix: string;
    limit: number;
    delimiter?: string;
    cursor?: string;
    startAfter?: string;
  } = { limit: maxKeys, prefix };
  if (delimiter) options.delimiter = delimiter;
  if (v2) {
    const token = c.req.query("continuation-token");
    if (token) options.cursor = token;
    const startAfter = c.req.query("start-after");
    if (!token && startAfter) options.startAfter = startAfter;
  } else {
    const marker = c.req.query("marker");
    if (marker) options.startAfter = marker;
  }
  const result = await backend.listObjects(options);
  const nextMarker =
    [result.objects.at(-1)?.key, result.delimitedPrefixes.at(-1)]
      .filter((value): value is string => value !== undefined)
      .sort()
      .at(-1) ?? "";
  return xml(
    c,
    <ListBucketResult xmlns={XMLNS}>
      <Name>{bucketName}</Name>
      <Prefix>{prefix}</Prefix>
      <KeyCount>{result.objects.length + result.delimitedPrefixes.length}</KeyCount>
      <MaxKeys>{maxKeys}</MaxKeys>
      {delimiter && <Delimiter>{delimiter}</Delimiter>}
      {v2
        ? c.req.query("start-after") && <StartAfter>{c.req.query("start-after")}</StartAfter>
        : c.req.query("marker") && <Marker>{c.req.query("marker")}</Marker>}
      {result.truncated ? (
        <>
          <IsTruncated>true</IsTruncated>
          {v2 ? (
            <NextContinuationToken>{result.cursor}</NextContinuationToken>
          ) : (
            <NextMarker>{nextMarker}</NextMarker>
          )}
        </>
      ) : (
        <IsTruncated>false</IsTruncated>
      )}
      {result.objects.map((object) => (
        <Contents>
          <Key>{object.key}</Key>
          <LastModified>{object.uploaded.toISOString()}</LastModified>
          <ETag>{quoteEtag(object.etag)}</ETag>
          <Size>{object.size}</Size>
          <StorageClass>STANDARD</StorageClass>
        </Contents>
      ))}
      {result.delimitedPrefixes.map((value) => (
        <CommonPrefixes>
          <Prefix>{value}</Prefix>
        </CommonPrefixes>
      ))}
    </ListBucketResult>,
  );
}

function parseMaxKeys(value: string | undefined): number {
  if (value === undefined || !/^\d+$/.test(value)) return 1000;
  return Math.min(Number(value), 1000);
}

function listBucketsResult(c: Context, bucketName: string): Response {
  return xml(
    c,
    <ListAllMyBucketsResult xmlns={XMLNS}>
      <Owner>
        <ID>s3-bridge</ID>
        <DisplayName>s3-bridge</DisplayName>
      </Owner>
      <Buckets>
        <Bucket>
          <Name>{bucketName}</Name>
          <CreationDate>1970-01-01T00:00:00.000Z</CreationDate>
        </Bucket>
      </Buckets>
    </ListAllMyBucketsResult>,
  );
}

async function deleteObjects(c: Context, backend: S3Backend): Promise<Response> {
  let parsed: unknown;
  try {
    const body = await c.req.text();
    SyntaxValidator.validate(body);
    parsed = parseXml(body);
  } catch {
    return s3Error("MalformedXML", "Malformed XML", c.req.path, 400);
  }
  const request = deleteObjectsRequestSchema.safeParse(parsed);
  if (!request.success) {
    return s3Error("InvalidRequest", "Object key is required", c.req.path, 400);
  }
  const entries = Array.isArray(request.data.Delete.Object)
    ? request.data.Delete.Object
    : [request.data.Delete.Object];
  const keys: string[] = [];
  for (const { Key: key } of entries) {
    if (key.length === 0) {
      return s3Error("InvalidRequest", "Object key is required", c.req.path, 400);
    }
    keys.push(key);
  }
  await backend.deleteObjects(keys);
  return xml(
    c,
    <DeleteResult xmlns={XMLNS}>
      {keys.map((key) => (
        <Deleted>
          <Key>{key}</Key>
        </Deleted>
      ))}
    </DeleteResult>,
  );
}

async function createMultipart(
  c: Context,
  backend: S3Backend,
  bucketName: string,
  key: string,
): Promise<Response> {
  try {
    const upload: S3MultipartUpload = await backend.createMultipartUpload(key, {
      httpMetadata: requestHttpMetadata(c.req.raw.headers),
    });
    return xml(
      c,
      <InitiateMultipartUploadResult xmlns={XMLNS}>
        <Bucket>{bucketName}</Bucket>
        <Key>{key}</Key>
        <UploadId>{upload.uploadId}</UploadId>
      </InitiateMultipartUploadResult>,
    );
  } catch {
    return s3Error(
      "InternalError",
      "We encountered an internal error. Please try again.",
      c.req.path,
      500,
    );
  }
}

async function uploadPart(
  c: Context,
  backend: S3Backend,
  key: string,
  partNumberValue: string | null,
  uploadId: string | null,
): Promise<Response> {
  const partNumber = Number(partNumberValue);
  if (
    !uploadId ||
    !/^\d+$/.test(partNumberValue ?? "") ||
    !Number.isInteger(partNumber) ||
    partNumber < 1 ||
    partNumber > 10000
  ) {
    return s3Error("InvalidPart", "The specified part is invalid.", c.req.path, 400);
  }
  try {
    const part = await backend.uploadPart(key, uploadId, partNumber, c.req.raw.body);
    return new Response(null, { status: 200, headers: { ETag: quoteEtag(part.etag) } });
  } catch {
    return s3Error("NoSuchUpload", "The specified upload does not exist.", c.req.path, 404);
  }
}

async function completeMultipart(
  c: Context,
  backend: S3Backend,
  bucketName: string,
  key: string,
  uploadId: string | null,
): Promise<Response> {
  if (!uploadId) return s3Error("InvalidRequest", "The uploadId is required.", c.req.path, 400);
  let parsed: unknown;
  try {
    const body = await c.req.text();
    SyntaxValidator.validate(body);
    parsed = parseXml(body);
  } catch {
    return s3Error(
      "MalformedXML",
      "The XML you provided was not well-formed or did not validate against our published schema.",
      c.req.path,
      400,
    );
  }
  const request = completeMultipartUploadRequestSchema.safeParse(parsed);
  if (!request.success) {
    return s3Error(
      "InvalidPart",
      "One or more of the specified parts could not be found.",
      c.req.path,
      400,
    );
  }
  const entries = Array.isArray(request.data.CompleteMultipartUpload.Part)
    ? request.data.CompleteMultipartUpload.Part
    : [request.data.CompleteMultipartUpload.Part];
  if (entries.length === 0)
    return s3Error("InvalidRequest", "At least one part is required.", c.req.path, 400);
  const parts: S3MultipartPart[] = [];
  let previous = 0;
  for (const { PartNumber: rawPartNumber, ETag: rawEtag } of entries) {
    const partNumber = Number(rawPartNumber);
    if (
      !/^\d+$/.test(rawPartNumber) ||
      !Number.isInteger(partNumber) ||
      partNumber < 1 ||
      partNumber > 10000 ||
      partNumber <= previous ||
      rawEtag.length === 0
    ) {
      return s3Error(
        "InvalidPart",
        "One or more of the specified parts could not be found.",
        c.req.path,
        400,
      );
    }
    previous = partNumber;
    parts.push({ partNumber, etag: rawEtag.replace(/^"|"$/g, "") });
  }
  try {
    const object = await backend.completeMultipartUpload(key, uploadId, parts);
    return xml(
      c,
      <CompleteMultipartUploadResult xmlns={XMLNS}>
        <Location>{new URL(c.req.url).origin + "/" + bucketName + "/" + key}</Location>
        <Bucket>{bucketName}</Bucket>
        <Key>{key}</Key>
        <ETag>{quoteEtag(object.etag)}</ETag>
      </CompleteMultipartUploadResult>,
    );
  } catch {
    return s3Error("NoSuchUpload", "The specified upload does not exist.", c.req.path, 404);
  }
}

async function abortMultipart(
  c: Context,
  backend: S3Backend,
  key: string,
  uploadId: string | null,
): Promise<Response> {
  if (!uploadId) return s3Error("InvalidRequest", "The uploadId is required.", c.req.path, 400);
  try {
    await backend.abortMultipartUpload(key, uploadId);
    return new Response(null, { status: 204 });
  } catch {
    return s3Error("NoSuchUpload", "The specified upload does not exist.", c.req.path, 404);
  }
}

const headerToMetadata = [
  ["Content-Type", "contentType"],
  ["Content-Language", "contentLanguage"],
  ["Content-Disposition", "contentDisposition"],
  ["Content-Encoding", "contentEncoding"],
  ["Cache-Control", "cacheControl"],
] as const;

function objectHeaders(object: S3ObjectMetadata): Headers {
  const headers = new Headers();
  const metadata = object.httpMetadata;
  for (const [header, field] of headerToMetadata) {
    const value = metadata[field];
    if (value !== undefined) headers.set(header, value);
  }
  if (metadata.cacheExpiry !== undefined)
    headers.set("Expires", metadata.cacheExpiry.toUTCString());
  headers.set("ETag", quoteEtag(object.etag));
  headers.set("Content-Length", String(object.size));
  headers.set("Last-Modified", object.uploaded.toUTCString());
  return headers;
}

function parseRange(value: string | null): S3ByteRange | "invalid" | undefined {
  if (value === null) return undefined;
  const match = /^bytes=([^,]+)$/.exec(value.trim());
  if (!match) return "invalid";
  const range = match[1];
  if (range === undefined) return "invalid";
  const suffix = /^-(\d+)$/.exec(range);
  if (suffix) {
    const length = Number(suffix[1]);
    return Number.isSafeInteger(length) && length > 0
      ? { kind: "suffix", suffix: length }
      : "invalid";
  }
  const bounded = /^(\d+)-(\d*)$/.exec(range);
  if (!bounded) return "invalid";
  const offset = Number(bounded[1]);
  if (!Number.isSafeInteger(offset)) return "invalid";
  if (bounded[2] === "") return { kind: "offset", offset };
  const end = Number(bounded[2]);
  if (!Number.isSafeInteger(end) || end < offset) return "invalid";
  return { kind: "offset", offset, length: end - offset + 1 };
}

const headerToCondition = [
  ["If-Match", "ifMatch", "string"],
  ["If-None-Match", "ifNoneMatch", "string"],
  ["If-Modified-Since", "ifModifiedSince", "date"],
  ["If-Unmodified-Since", "ifUnmodifiedSince", "date"],
] as const;

function requestConditions(headers: Headers): S3Conditions | undefined {
  const conditions: { -readonly [K in keyof S3Conditions]: S3Conditions[K] } = {};
  for (const [header, field, typ] of headerToCondition) {
    const value = headers.get(header);
    if (value === null) continue;
    if (typ === "date") {
      const date = parseDate(value);
      if (date) conditions[field] = date;
    } else {
      conditions[field] = value;
    }
  }
  return Object.keys(conditions).length > 0 ? conditions : undefined;
}

function parseDate(value: string | null): Date | undefined {
  if (value === null) return undefined;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? undefined : date;
}

function requestHttpMetadata(headers: Headers): S3HttpMetadata {
  const metadata: { -readonly [K in keyof S3HttpMetadata]: S3HttpMetadata[K] } = {};
  for (const [header, field] of headerToMetadata) {
    const value = headers.get(header);
    if (value === null) continue;
    metadata[field] = value;
  }
  const expires = parseDate(headers.get("Expires"));
  if (expires) metadata.cacheExpiry = expires;
  return metadata;
}

function quoteEtag(etag: string): string {
  return etag.startsWith('"') ? etag : `"${etag}"`;
}

function parseXml(body: string): unknown {
  return new XMLParser({
    ignoreAttributes: true,
    removeNSPrefix: true,
    parseTagValue: false,
    htmlEntities: true,
  }).parse(body);
}

function xml(c: Context, body: JSX.Element): Response {
  if (body instanceof Promise) throw new Error("Asynchronous XML is not supported");
  c.header("Content-Type", "application/xml; charset=utf-8");
  return c.text(`<?xml version="1.0" encoding="UTF-8"?>${resolveCallbackSync(body)}`);
}

function renderXML(body: JSX.Element, status = 200): Response {
  if (body instanceof Promise) throw new Error("Asynchronous XML is not supported");
  return new Response(`<?xml version="1.0" encoding="UTF-8"?>${resolveCallbackSync(body)}`, {
    status,
    headers: { "Content-Type": "application/xml; charset=utf-8" },
  });
}

function notImplemented(resource: string): Response {
  return s3Error(
    "NotImplemented",
    "A header you provided implies functionality that is not implemented.",
    resource,
    501,
  );
}

function s3Error(code: string, message: string, resource: string, status: number): Response {
  return renderXML(
    <ErrorElement>
      <Code>{code}</Code>
      <Message>{message}</Message>
      <Resource>{resource}</Resource>
      <RequestId>s3-bridge</RequestId>
    </ErrorElement>,
    status,
  );
}
