import { RpcTarget } from "cloudflare:workers";
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

const CHUNK_SIZE = 1024 * 1024;

export class DOS3Backend implements S3Backend {
  constructor(private readonly getStore: () => Rpc.Result<DOS3Store>) {}

  async getObject(key: string, options: S3GetOptions) {
    using result = await this.getStore().getObject(key, options);
    return result;
  }

  async putObject(key: string, body: ReadableStream<Uint8Array> | null, options: S3PutOptions) {
    const upload = this.getStore().createUpload(key, options);
    try {
      using part = await upload.uploadPart(1, body);
      return await upload.complete([part]);
    } catch (err) {
      try {
        await upload.abort();
      } catch (abortErr) {
        throw new AggregateError([err, abortErr], "Failed to put object and abort upload");
      }
      throw err;
    }
  }

  async deleteObjects(keys: readonly string[]) {
    await this.getStore().deleteObjects(keys);
  }

  async listObjects(options: S3ListOptions) {
    using result = await this.getStore().listObjects(options);
    return result;
  }

  async createMultipartUpload(key: string, options: S3PutOptions): Promise<S3MultipartUpload> {
    const id = await this.getStore().createUpload(key, options).getID();
    return { uploadId: id };
  }

  async uploadPart(
    key: string,
    uploadId: string,
    partNumber: number,
    body: ReadableStream<Uint8Array> | null,
  ) {
    using result = await this.getStore().getUpload(key, uploadId).uploadPart(partNumber, body);
    return result;
  }

  async completeMultipartUpload(key: string, uploadId: string, parts: readonly S3MultipartPart[]) {
    using result = await this.getStore().getUpload(key, uploadId).complete(parts);
    return result;
  }

  async abortMultipartUpload(key: string, uploadId: string) {
    await this.getStore().getUpload(key, uploadId).abort();
  }
}

type ObjectRow = {
  key: string;
  size: number;
  etag: string;
  modified_at_ms: number;
  content_type: string | null;
  content_language: string | null;
  content_disposition: string | null;
  content_encoding: string | null;
  cache_control: string | null;
  cache_expires_at_ms: number | null;
};

export class DOS3Store extends RpcTarget {
  constructor(private readonly storage: DurableObjectStorage) {
    super();
    migrate(storage);
  }
  private row(key: string) {
    return this.storage.sql
      .exec<ObjectRow>(
        `
          SELECT o.key,
                 u.size,
                 u.etag,
                 u.content_type,
                 u.content_language,
                 u.content_disposition,
                 u.content_encoding,
                 u.cache_control,
                 u.cache_expires_at_ms,
                 o.modified_at_ms
            FROM s3_objects AS o
            JOIN s3_uploads AS u ON u.id = o.upload_id
           WHERE o.key = ?
        `,
        key,
      )
      .toArray()
      .at(0);
  }
  private id(key: string) {
    return this.storage.sql
      .exec<{ upload_id: string }>("SELECT upload_id FROM s3_objects WHERE key = ?", key)
      .toArray()
      .at(0)?.upload_id;
  }
  async getObject(key: string, options: S3GetOptions): Promise<S3GetResult> {
    const row = this.row(key);
    if (!row) return { kind: "not-found" };
    const object = metadata(row);
    const c = options.conditions;
    const matches = (s: string) =>
      s === "*" ||
      s
        .split(",")
        .map((v) => v.trim().replace(/^"|"$/g, ""))
        .includes(row.etag);
    if (
      (c?.ifMatch !== undefined && !matches(c.ifMatch)) ||
      (c?.ifUnmodifiedSince !== undefined &&
        Math.floor(c.ifUnmodifiedSince.getTime() / 1000) < Math.floor(row.modified_at_ms / 1000))
    )
      return { kind: "precondition-failed", object };
    if (
      (c?.ifNoneMatch !== undefined && matches(c.ifNoneMatch)) ||
      (c?.ifNoneMatch === undefined &&
        c?.ifModifiedSince !== undefined &&
        Math.floor(c.ifModifiedSince.getTime() / 1000) >= Math.floor(row.modified_at_ms / 1000))
    )
      return { kind: "not-modified", object };
    let start = 0,
      end = row.size;
    if (options.range?.kind === "offset") {
      const { offset, length } = options.range;
      if (
        !Number.isSafeInteger(offset) ||
        offset < 0 ||
        (length !== undefined &&
          (!Number.isSafeInteger(length) || length <= 0 || !Number.isSafeInteger(offset + length)))
      )
        return { kind: "range-not-satisfiable", size: row.size };
      start = offset;
      end = Math.min(row.size, length === undefined ? row.size : offset + length);
    } else if (options.range?.kind === "suffix") {
      if (!Number.isSafeInteger(options.range.suffix) || options.range.suffix <= 0)
        return { kind: "range-not-satisfiable", size: row.size };
      start = Math.max(0, row.size - options.range.suffix);
    }
    if (options.range && (start >= row.size || end <= start))
      return { kind: "range-not-satisfiable", size: row.size };

    const upload = this.id(key)!;
    let offset = 0;
    let cursor = { part: -1, split: -1 };
    const body = new ReadableStream<Uint8Array>({
      pull: (controller) => {
        const chunks = this.storage.sql.exec<{
          part_number: number;
          part_split: number;
          size: number;
          data: ArrayBuffer;
        }>(
          `
          SELECT part_number, part_split, size, data FROM s3_chunks
          WHERE upload_id = ? AND (part_number > ? OR (part_number = ? AND part_split > ?))
          ORDER BY part_number ASC, part_split ASC`,
          upload,
          cursor.part,
          cursor.part,
          cursor.split,
        );
        for (const chunk of chunks) {
          if (offset >= end) break;
          if (offset + chunk.size <= start) {
            offset += chunk.size;
            continue;
          }

          controller.enqueue(
            new Uint8Array(chunk.data).subarray(
              Math.max(0, start - offset),
              Math.min(chunk.size, end - offset),
            ),
          );
          offset += chunk.size;

          if (controller.desiredSize == null || controller.desiredSize <= 0) {
            cursor = { part: chunk.part_number, split: chunk.part_split };
            return;
          }
        }
        controller.close();
      },
    });
    return {
      kind: "found",
      object: {
        ...object,
        body,
        ...(options.range ? { range: { offset: start, length: end - start } } : {}),
      },
    };
  }

  deleteObjects(keys: readonly string[]) {
    this.storage.transactionSync(() => {
      for (const key of keys) {
        const id = this.id(key);
        this.storage.sql.exec("DELETE FROM s3_objects WHERE key = ?", key);
        if (id) this.cleanup(id);
      }
    });
    return;
  }

  listObjects(o: S3ListOptions): S3ListResult {
    const after = o.cursor !== undefined ? decodeCursor(o.cursor) : (o.startAfter ?? "");
    if (o.limit === 0) return { objects: [], delimitedPrefixes: [], truncated: false };
    const rows = this.storage.sql.exec<ObjectRow>(
      `
          SELECT o.key,
                 u.size,
                 u.etag,
                 u.content_type,
                 u.content_language,
                 u.content_disposition,
                 u.content_encoding,
                 u.cache_control,
                 u.cache_expires_at_ms,
                 o.modified_at_ms
            FROM s3_objects AS o
            JOIN s3_uploads AS u ON u.id = o.upload_id
           WHERE substr(o.key, 1, length(?)) = ?
             AND o.key > ?
        ORDER BY o.key`,
      o.prefix,
      o.prefix,
      after,
    );
    const objects: S3ObjectMetadata[] = [],
      prefixes: string[] = [],
      seen = new Set<string>();
    let lastKey = after;
    let group: string | undefined;
    let limitReached = false;
    let truncated = false;
    for (const row of rows) {
      const rest = row.key.slice(o.prefix.length),
        i = o.delimiter ? rest.indexOf(o.delimiter) : -1;
      if (i >= 0) {
        const p = o.prefix + rest.slice(0, i + 1);
        if (group === p) {
          lastKey = row.key;
          continue;
        }
        if (limitReached) {
          truncated = true;
          break;
        }
        group = p;
        // A v1 marker can be a common prefix.  Its whole group has already
        // been returned, so consume it before considering the next item.
        const skip = after === p && !o.cursor;
        if (!skip && !seen.has(p)) {
          seen.add(p);
          prefixes.push(p);
        }
        lastKey = row.key;
        limitReached = objects.length + prefixes.length >= o.limit;
      } else {
        if (limitReached) {
          truncated = true;
          break;
        }
        group = undefined;
        objects.push(metadata(row));
        lastKey = row.key;
        limitReached = objects.length + prefixes.length >= o.limit;
      }
    }
    return truncated
      ? {
          objects,
          delimitedPrefixes: prefixes,
          truncated: true,
          cursor: encodeCursor(lastKey),
        }
      : { objects, delimitedPrefixes: prefixes, truncated: false };
  }

  createUpload(key: string, options: S3PutOptions): DOS3Upload {
    const id = crypto.randomUUID();
    insertUpload(this.storage.sql, id, key, null, Date.now(), options);
    return new DOS3Upload(this.storage, key, id);
  }

  getUpload(key: string, id: string): DOS3Upload {
    return new DOS3Upload(this.storage, key, id);
  }

  private cleanup(id: string) {
    if (
      !this.storage.sql.exec("SELECT 1 FROM s3_objects WHERE upload_id = ?", id).toArray().length
    ) {
      this.storage.sql.exec("DELETE FROM s3_uploads WHERE id = ?", id);
      this.storage.sql.exec("DELETE FROM s3_parts WHERE upload_id = ?", id);
      this.storage.sql.exec("DELETE FROM s3_chunks WHERE upload_id = ?", id);
    }
  }
}

class DOS3Upload extends RpcTarget {
  constructor(
    private readonly storage: DurableObjectStorage,
    readonly key: string,
    readonly id: string,
  ) {
    super();
  }

  getID() {
    return this.id;
  }

  abort() {
    const deleted = this.storage.transactionSync(() => {
      if (!this.isPending()) return false;
      this.storage.sql.exec("DELETE FROM s3_uploads WHERE id = ?", this.id);
      return true;
    });
    if (!deleted) throw new Error("No such upload");
  }

  // TODO: Workers RPC doesn't support placing RpcPromises into the parameters.
  // After that is fixed, this can be made to return a RpcPromise<UploadPart extends RpcTarget> instead of a plain part object.
  async uploadPart(partNumber: number, body: ReadableStream<Uint8Array> | null) {
    // Upload the part in chunks, without linking them to the part yet.
    const chunkIDs: string[] = [];
    try {
      let size = 0;
      if (body != null) {
        let buffer = new Uint8Array(0);
        let bufferedChunks: Array<Uint8Array> = [];
        let bufferedSize = 0;
        let split = 0;

        const insertChunk = (chunk: Uint8Array) => {
          const chunkID = crypto.randomUUID();
          this.storage.sql.exec(
            "INSERT INTO s3_chunks (id, part_number, part_split, size, data) VALUES (?, ?, ?, ?, ?)",
            chunkID,
            partNumber,
            split++,
            chunk.byteLength,
            chunk,
          );
          chunkIDs.push(chunkID);
        };

        const reader = body.getReader();
        try {
          for (;;) {
            const { done, value } = await reader.read();

            if (done || bufferedSize + value.byteLength >= CHUNK_SIZE) {
              if (bufferedSize > 0) {
                if (buffer.byteLength < bufferedSize) {
                  buffer = new Uint8Array(bufferedSize);
                }
                let offset = 0;
                for (const chunk of bufferedChunks) {
                  buffer.set(chunk, offset);
                  offset += chunk.byteLength;
                }
                insertChunk(buffer.subarray(0, bufferedSize));
                bufferedChunks = [];
                bufferedSize = 0;
              }
              if (done) break;
            }

            size += value.byteLength;
            for (let i = 0; i < value.byteLength; i += CHUNK_SIZE) {
              const chunk = value.subarray(i, i + CHUNK_SIZE);
              if (chunk.byteLength === CHUNK_SIZE) {
                insertChunk(chunk);
              } else {
                bufferedChunks.push(chunk);
                bufferedSize += chunk.byteLength;
              }
            }
          }
        } finally {
          reader.releaseLock();
        }
      }

      const result = this.storage.transactionSync(() => {
        if (!this.isPending()) return { kind: "no-such-upload" } as const;

        // Delete any existing chunks and part for this part number, if any.
        this.storage.sql.exec(
          "DELETE FROM s3_chunks WHERE upload_id = ? AND part_number = ?",
          this.id,
          partNumber,
        );
        this.storage.sql.exec(
          "DELETE FROM s3_parts WHERE upload_id = ? AND part_number = ?",
          this.id,
          partNumber,
        );

        // Link the new chunks to the part.
        for (const chunkID of chunkIDs) {
          this.storage.sql.exec(
            "UPDATE s3_chunks SET upload_id = ? WHERE id = ?",
            this.id,
            chunkID,
          );
        }

        const etag = crypto.randomUUID();
        this.storage.sql.exec(
          `
          INSERT INTO s3_parts (upload_id, part_number, etag, size)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(upload_id, part_number) DO UPDATE SET
            etag = excluded.etag,
            size = excluded.size`,
          this.id,
          partNumber,
          etag,
          size,
        );
        return { kind: "uploaded", etag } as const;
      });
      if (result.kind === "no-such-upload") throw new Error("No such upload");
      return { partNumber, etag: result.etag };
    } catch (err) {
      try {
        this.storage.transactionSync(() => {
          for (const chunkID of chunkIDs) {
            this.storage.sql.exec("DELETE FROM s3_chunks WHERE id = ?", chunkID);
          }
        });
      } catch (cleanupErr) {
        throw new AggregateError([err, cleanupErr], "Failed to upload part and cleanup chunks");
      }
      throw err;
    }
  }

  complete(parts: readonly S3MultipartPart[]) {
    const now = Date.now(),
      etag = crypto.randomUUID();
    const result = this.storage.transactionSync(() => {
      if (!this.isPending()) return { kind: "no-such-upload" } as const;
      const actual = this.storage.sql
        .exec<{ part_number: number; etag: string; size: number }>(
          "SELECT part_number, etag, size FROM s3_parts WHERE upload_id = ? ORDER BY part_number",
          this.id,
        )
        .toArray();
      const invalid =
        parts.length === 0 ||
        parts.some(
          (p, i) =>
            !Number.isSafeInteger(p.partNumber) ||
            p.partNumber <= 0 ||
            !p.etag ||
            (i > 0 && parts[i - 1]!.partNumber >= p.partNumber) ||
            !actual.some((a) => a.part_number === p.partNumber && a.etag === p.etag),
        );
      if (invalid) return { kind: "invalid" } as const;
      let size = 0;
      {
        for (const part of parts) {
          const actualPart = actual.find((candidate) => candidate.part_number === part.partNumber);
          if (
            !actualPart ||
            !Number.isSafeInteger(actualPart.size) ||
            !Number.isSafeInteger(size + actualPart.size)
          )
            return { kind: "invalid" } as const;
          size += actualPart.size;
        }
      }
      const old = uploadIDFromKey(this.storage, this.key);
      this.storage.sql.exec(
        "UPDATE s3_uploads SET etag = ?, size = ? WHERE id = ?",
        etag,
        size,
        this.id,
      );
      this.storage.sql.exec(
        `
          INSERT INTO s3_objects (key, upload_id, modified_at_ms) VALUES (?, ?, ?)
          ON CONFLICT(key) DO UPDATE SET upload_id = excluded.upload_id, modified_at_ms = excluded.modified_at_ms`,
        this.key,
        this.id,
        now,
      );
      for (const p of actual) {
        if (!parts.some((requested) => requested.partNumber === p.part_number)) {
          this.storage.sql.exec(
            "DELETE FROM s3_parts WHERE upload_id = ? AND part_number = ?",
            this.id,
            p.part_number,
          );
          this.storage.sql.exec(
            "DELETE FROM s3_chunks WHERE upload_id = ? AND part_number = ?",
            this.id,
            p.part_number,
          );
        }
      }
      if (old && old !== this.id) cleanupUpload(this.storage, old);
      return { kind: "completed", size } as const;
    });
    if (result.kind === "no-such-upload") throw new Error("No such upload");
    if (result.kind === "invalid") throw new Error("Invalid multipart parts");
    return metadata(readObject(this.storage, this.key)!);
  }

  private isPending() {
    const upload = this.storage.sql
      .exec<{ id: string }>("SELECT id FROM s3_uploads WHERE id = ? AND key = ?", this.id, this.key)
      .toArray()
      .at(0);
    if (!upload) return false;

    const object = this.storage.sql
      .exec<{ upload_id: string }>(
        "SELECT upload_id FROM s3_objects WHERE upload_id = ? LIMIT 1",
        this.id,
      )
      .toArray()
      .at(0);
    return !object;
  }
}

function metadata(r: ObjectRow): S3ObjectMetadata {
  return {
    key: r.key,
    size: r.size,
    etag: r.etag,
    uploaded: new Date(r.modified_at_ms),
    httpMetadata: {
      contentType: r.content_type ?? undefined,
      contentLanguage: r.content_language ?? undefined,
      contentDisposition: r.content_disposition ?? undefined,
      contentEncoding: r.content_encoding ?? undefined,
      cacheControl: r.cache_control ?? undefined,
      cacheExpiry: r.cache_expires_at_ms == null ? undefined : new Date(r.cache_expires_at_ms),
    },
  };
}

function uploadIDFromKey(storage: DurableObjectStorage, key: string) {
  return storage.sql
    .exec<{ upload_id: string }>("SELECT upload_id FROM s3_objects WHERE key = ?", key)
    .toArray()
    .at(0)?.upload_id;
}

function readObject(storage: DurableObjectStorage, key: string) {
  return storage.sql
    .exec<ObjectRow>(
      `
        SELECT o.key,
               u.size,
               u.etag,
               u.content_type,
               u.content_language,
               u.content_disposition,
               u.content_encoding,
               u.cache_control,
               u.cache_expires_at_ms,
               o.modified_at_ms
          FROM s3_objects AS o
          JOIN s3_uploads AS u ON u.id = o.upload_id
         WHERE o.key = ?`,
      key,
    )
    .toArray()
    .at(0);
}

function cleanupUpload(storage: DurableObjectStorage, id: string) {
  if (!storage.sql.exec("SELECT 1 FROM s3_objects WHERE upload_id = ?", id).toArray().length)
    storage.sql.exec("DELETE FROM s3_uploads WHERE id = ?", id);
}

function insertUpload(
  sql: SqlStorage,
  id: string,
  key: string,
  etag: string | null,
  now: number,
  o: S3PutOptions,
) {
  const m = o.httpMetadata;
  sql.exec(
    `
      INSERT INTO s3_uploads
        (id, key, etag, size, created_at_ms, content_type,
         content_language, content_disposition, content_encoding,
         cache_control, cache_expires_at_ms)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    id,
    key,
    etag,
    0,
    now,
    m.contentType ?? null,
    m.contentLanguage ?? null,
    m.contentDisposition ?? null,
    m.contentEncoding ?? null,
    m.cacheControl ?? null,
    m.cacheExpiry?.getTime() ?? null,
  );
}

// Cursor format: the object-key boundary string's UTF-8 bytes encoded as
// RFC 4648 URL-safe Base64, without padding.
function encodeCursor(s: string) {
  return new TextEncoder().encode(s).toBase64({ alphabet: "base64url", omitPadding: true });
}

// Decode an unpadded RFC 4648 Base64URL cursor strictly, then decode its bytes
// as fatal UTF-8; malformed Base64URL and malformed UTF-8 are rejected.
function decodeCursor(s: string) {
  return new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(
    Uint8Array.fromBase64(s, { alphabet: "base64url" }),
  );
}

const MIGRATIONS: ReadonlyArray<{ version: number; up: (sql: SqlStorage) => void }> = [
  {
    version: 1,
    up(sql) {
      sql.exec(
        `
          CREATE TABLE s3_uploads (
            id TEXT PRIMARY KEY,
            key TEXT NOT NULL,
            etag TEXT,
            size INTEGER NOT NULL DEFAULT 0,
            created_at_ms INTEGER NOT NULL,
            content_type TEXT,
            content_language TEXT,
            content_disposition TEXT,
            content_encoding TEXT,
            cache_control TEXT,
            cache_expires_at_ms INTEGER
          )`,
      );
      sql.exec(
        `
          CREATE TABLE s3_objects (
            key TEXT PRIMARY KEY,
            upload_id TEXT NOT NULL,
            modified_at_ms INTEGER NOT NULL
          )`,
      );
      sql.exec(
        `
          CREATE TABLE s3_parts (
            upload_id TEXT NOT NULL,
            part_number INTEGER NOT NULL,
            etag TEXT NOT NULL,
            size INTEGER NOT NULL,
            PRIMARY KEY(upload_id, part_number)
          )`,
      );
      sql.exec(
        `
          CREATE TABLE s3_chunks (
            id TEXT PRIMARY KEY,
            upload_id TEXT,
            part_number INTEGER NOT NULL,
            part_split INTEGER NOT NULL,
            size INTEGER NOT NULL,
            data BLOB NOT NULL
          )`,
      );
    },
  },
];

const MAX_MIGRATION_VERSION = Math.max(...MIGRATIONS.map(({ version }) => version));

function migrate(storage: DurableObjectStorage, desiredVersion = MAX_MIGRATION_VERSION) {
  storage.sql.exec("PRAGMA foreign_keys = ON");
  storage.sql.exec(
    `
      CREATE TABLE IF NOT EXISTS s3_migrations (
        version INTEGER PRIMARY KEY,
        applied_at_ms INTEGER NOT NULL
      )`,
  );
  const currentVersion =
    storage.sql
      .exec<{ version: number }>(
        "SELECT version FROM s3_migrations ORDER BY applied_at_ms DESC LIMIT 1",
      )
      .toArray()
      .at(0)?.version ?? 0;
  for (const migration of MIGRATIONS.toSorted((a, b) => a.version - b.version)) {
    if (migration.version <= currentVersion) continue;
    if (migration.version > desiredVersion) break;
    storage.transactionSync(() => {
      migration.up(storage.sql);
      storage.sql.exec("INSERT INTO s3_migrations VALUES (?, ?)", migration.version, Date.now());
    });
  }
}
