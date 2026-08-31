import type {
  S3Backend,
  S3ByteRange,
  S3Conditions,
  S3GetOptions,
  S3GetResult,
  S3HttpMetadata,
  S3ListOptions,
  S3ListResult,
  S3MultipartPart,
  S3MultipartUpload,
  S3ObjectMetadata,
  S3PutOptions,
} from "./s3-backend";

export class R2S3Backend implements S3Backend {
  readonly #bucket: R2Bucket;

  constructor(bucket: R2Bucket) {
    this.#bucket = bucket;
  }

  async getObject(key: string, options: S3GetOptions): Promise<S3GetResult> {
    const r2Options: R2GetOptions = {};
    if (options.range) r2Options.range = toR2Range(options.range);
    if (options.conditions) r2Options.onlyIf = toR2Conditional(options.conditions);
    const object = await this.#bucket.get(key, r2Options);
    if (!object) return { kind: "not-found" };

    const metadata = toMetadata(object);
    if (!("body" in object)) {
      const conditions = options.conditions;
      const kind =
        conditions?.ifMatch !== undefined || conditions?.ifUnmodifiedSince !== undefined
          ? "precondition-failed"
          : "not-modified";
      return { kind, object: metadata };
    }

    return {
      kind: "found",
      object: {
        ...metadata,
        body: object.body,
        ...(object.range && "offset" in object.range && object.range.length !== undefined
          ? { range: { offset: object.range.offset, length: object.range.length } }
          : {}),
      },
    };
  }

  async putObject(
    key: string,
    body: ReadableStream<Uint8Array> | null,
    options: S3PutOptions,
  ): Promise<S3ObjectMetadata> {
    return toMetadata(
      await this.#bucket.put(key, body, { httpMetadata: toR2Metadata(options.httpMetadata) }),
    );
  }

  async deleteObjects(keys: readonly string[]): Promise<void> {
    await this.#bucket.delete([...keys]);
  }

  async listObjects(options: S3ListOptions): Promise<S3ListResult> {
    const result = await this.#bucket.list({
      prefix: options.prefix,
      limit: options.limit,
      ...(options.delimiter !== undefined ? { delimiter: options.delimiter } : {}),
      ...(options.cursor !== undefined ? { cursor: options.cursor } : {}),
      ...(options.startAfter !== undefined ? { startAfter: options.startAfter } : {}),
    });
    const converted = {
      objects: result.objects.map(toMetadata),
      delimitedPrefixes: result.delimitedPrefixes,
    };
    return result.truncated
      ? { ...converted, truncated: true, cursor: result.cursor }
      : { ...converted, truncated: false };
  }

  async createMultipartUpload(key: string, options: S3PutOptions): Promise<S3MultipartUpload> {
    const upload = await this.#bucket.createMultipartUpload(key, {
      httpMetadata: toR2Metadata(options.httpMetadata),
    });
    return { uploadId: upload.uploadId };
  }

  async uploadPart(
    key: string,
    uploadId: string,
    partNumber: number,
    body: ReadableStream<Uint8Array> | null,
  ): Promise<S3MultipartPart> {
    const part = await this.#bucket
      .resumeMultipartUpload(key, uploadId)
      .uploadPart(partNumber, body ?? "");
    return { partNumber: part.partNumber, etag: part.etag };
  }

  async completeMultipartUpload(
    key: string,
    uploadId: string,
    parts: readonly S3MultipartPart[],
  ): Promise<S3ObjectMetadata> {
    const uploadedParts = parts.map(({ partNumber, etag }) => ({ partNumber, etag }));
    return toMetadata(
      await this.#bucket.resumeMultipartUpload(key, uploadId).complete(uploadedParts),
    );
  }

  async abortMultipartUpload(key: string, uploadId: string): Promise<void> {
    await this.#bucket.resumeMultipartUpload(key, uploadId).abort();
  }
}

function toMetadata(object: R2Object): S3ObjectMetadata {
  return {
    key: object.key,
    size: object.size,
    etag: object.etag,
    uploaded: object.uploaded,
    httpMetadata: fromR2Metadata(object.httpMetadata),
  };
}

function fromR2Metadata(metadata: R2HTTPMetadata | undefined): S3HttpMetadata {
  return {
    ...(metadata?.contentType !== undefined ? { contentType: metadata.contentType } : {}),
    ...(metadata?.contentLanguage !== undefined
      ? { contentLanguage: metadata.contentLanguage }
      : {}),
    ...(metadata?.contentDisposition !== undefined
      ? { contentDisposition: metadata.contentDisposition }
      : {}),
    ...(metadata?.contentEncoding !== undefined
      ? { contentEncoding: metadata.contentEncoding }
      : {}),
    ...(metadata?.cacheControl !== undefined ? { cacheControl: metadata.cacheControl } : {}),
    ...(metadata?.cacheExpiry !== undefined ? { cacheExpiry: metadata.cacheExpiry } : {}),
  };
}

function toR2Metadata(metadata: S3HttpMetadata): R2HTTPMetadata {
  return {
    ...(metadata.contentType !== undefined ? { contentType: metadata.contentType } : {}),
    ...(metadata.contentLanguage !== undefined
      ? { contentLanguage: metadata.contentLanguage }
      : {}),
    ...(metadata.contentDisposition !== undefined
      ? { contentDisposition: metadata.contentDisposition }
      : {}),
    ...(metadata.contentEncoding !== undefined
      ? { contentEncoding: metadata.contentEncoding }
      : {}),
    ...(metadata.cacheControl !== undefined ? { cacheControl: metadata.cacheControl } : {}),
    ...(metadata.cacheExpiry !== undefined ? { cacheExpiry: metadata.cacheExpiry } : {}),
  };
}

function toR2Range(range: S3ByteRange): R2Range {
  return range.kind === "suffix"
    ? { suffix: range.suffix }
    : { offset: range.offset, ...(range.length !== undefined ? { length: range.length } : {}) };
}

function toR2Conditional(conditions: S3Conditions): R2Conditional {
  return {
    ...(conditions.ifMatch !== undefined ? { etagMatches: conditions.ifMatch } : {}),
    ...(conditions.ifNoneMatch !== undefined ? { etagDoesNotMatch: conditions.ifNoneMatch } : {}),
    ...(conditions.ifModifiedSince !== undefined
      ? { uploadedAfter: conditions.ifModifiedSince }
      : {}),
    ...(conditions.ifUnmodifiedSince !== undefined
      ? { uploadedBefore: conditions.ifUnmodifiedSince }
      : {}),
    secondsGranularity: true,
  };
}
