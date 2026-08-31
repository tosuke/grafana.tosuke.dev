export type S3HttpMetadata = {
  readonly contentType?: string;
  readonly contentLanguage?: string;
  readonly contentDisposition?: string;
  readonly contentEncoding?: string;
  readonly cacheControl?: string;
  readonly cacheExpiry?: Date;
};

export type S3ObjectMetadata = {
  readonly key: string;
  readonly size: number;
  readonly etag: string; // invariant: raw/unquoted
  readonly uploaded: Date;
  readonly httpMetadata: S3HttpMetadata;
};

export type S3ByteRange =
  | { readonly kind: "offset"; readonly offset: number; readonly length?: number }
  | { readonly kind: "suffix"; readonly suffix: number };

export type S3Conditions = {
  readonly ifMatch?: string;
  readonly ifNoneMatch?: string;
  readonly ifModifiedSince?: Date;
  readonly ifUnmodifiedSince?: Date;
};

export type S3ObjectBody = S3ObjectMetadata & {
  readonly body: ReadableStream<Uint8Array>;
  readonly range?: { readonly offset: number; readonly length: number };
};

export type S3GetResult =
  | { readonly kind: "found"; readonly object: S3ObjectBody }
  | { readonly kind: "not-found" }
  | { readonly kind: "not-modified"; readonly object: S3ObjectMetadata }
  | { readonly kind: "precondition-failed"; readonly object: S3ObjectMetadata };

export type S3GetOptions = {
  readonly range?: S3ByteRange;
  readonly conditions?: S3Conditions;
};

export type S3ListOptions = {
  readonly prefix: string;
  readonly limit: number;
  readonly delimiter?: string;
  readonly cursor?: string;
  readonly startAfter?: string;
};

type S3ListResultBase = {
  readonly objects: readonly S3ObjectMetadata[];
  readonly delimitedPrefixes: readonly string[];
};

export type S3ListResult = S3ListResultBase &
  ({ readonly truncated: false } | { readonly truncated: true; readonly cursor: string });

export type S3PutOptions = { readonly httpMetadata: S3HttpMetadata };

export type S3MultipartPart = {
  readonly partNumber: number;
  readonly etag: string; // invariant: raw/unquoted
};

export type S3MultipartUpload = { readonly uploadId: string };

export interface S3Backend {
  getObject(key: string, options: S3GetOptions): Promise<S3GetResult>;
  putObject(
    key: string,
    body: ReadableStream<Uint8Array> | null,
    options: S3PutOptions,
  ): Promise<S3ObjectMetadata>;
  deleteObjects(keys: readonly string[]): Promise<void>;
  listObjects(options: S3ListOptions): Promise<S3ListResult>;
  createMultipartUpload(key: string, options: S3PutOptions): Promise<S3MultipartUpload>;
  uploadPart(
    key: string,
    uploadId: string,
    partNumber: number,
    body: ReadableStream<Uint8Array> | null,
  ): Promise<S3MultipartPart>;
  completeMultipartUpload(
    key: string,
    uploadId: string,
    parts: readonly S3MultipartPart[],
  ): Promise<S3ObjectMetadata>;
  abortMultipartUpload(key: string, uploadId: string): Promise<void>;
}
