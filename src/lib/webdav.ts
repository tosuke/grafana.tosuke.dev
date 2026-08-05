import { Hono } from "hono";

const DAV_HEADERS = {
  Allow: "OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, MOVE, COPY, PROPFIND",
  DAV: "1",
};
const CHUNK_SIZE = 1.5 * 1024 * 1024;

type File = {
  inodeId: number;
  contentLength: number;
  contentType: string | null;
  etag: string;
  lastModified: number;
};

type Resource = { kind: "file"; file: File } | { kind: "collection" };

type Migration = {
  version: number;
  statements: string[];
};

const migrations: Migration[] = [
  {
    version: 1,
    statements: [
      `
        CREATE TABLE webdav_inodes (
          id INTEGER PRIMARY KEY,
          content_length INTEGER NOT NULL,
          content_type TEXT,
          etag TEXT NOT NULL,
          last_modified INTEGER NOT NULL
        )
      `,
      `
        CREATE TABLE webdav_chunks (
          inode_id INTEGER NOT NULL,
          chunk_index INTEGER NOT NULL,
          body BLOB NOT NULL,
          PRIMARY KEY (inode_id, chunk_index)
        )
      `,
      `
        CREATE TABLE webdav_paths (
          path TEXT PRIMARY KEY,
          inode_id INTEGER NOT NULL
        )
      `,
    ],
  },
];

function migrate(storage: DurableObjectStorage): void {
  storage.transactionSync(() => {
    const sql = storage.sql;
    sql.exec(`
      CREATE TABLE IF NOT EXISTS webdav_migrations (
        version INTEGER PRIMARY KEY,
        applied_at INTEGER NOT NULL
      )
    `);
    const applied = new Set(
      sql
        .exec<{ version: number }>("SELECT version FROM webdav_migrations")
        .toArray()
        .map((migration) => migration.version),
    );
    for (const migration of migrations) {
      if (applied.has(migration.version)) continue;
      for (const statement of migration.statements) sql.exec(statement);
      sql.exec(
        "INSERT INTO webdav_migrations (version, applied_at) VALUES (?, ?)",
        migration.version,
        Date.now(),
      );
    }
  });
}

function escapeXml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function xml(strings: TemplateStringsArray, ...values: unknown[]): string {
  return String.raw({ raw: strings }, ...values.map((value) => escapeXml(String(value))));
}

function keyFromPathname(pathname: string): string | null {
  const parts = pathname.split("/").filter(Boolean);
  const decoded = parts.map((part) => {
    try {
      return decodeURIComponent(part);
    } catch {
      return null;
    }
  });
  if (
    decoded.some(
      (part) =>
        part === null ||
        part.length === 0 ||
        part === "." ||
        part === ".." ||
        part.includes("/") ||
        part.includes("\0"),
    )
  ) {
    return null;
  }
  return decoded.join("/");
}

function collectionPrefix(key: string): string {
  return `${key}/`;
}

function likePrefix(prefix: string): string {
  return `${prefix.replaceAll("\\", "\\\\").replaceAll("%", "\\%").replaceAll("_", "\\_")}%`;
}

function fileFrom(row: {
  inode_id: number;
  content_length: number;
  content_type: string | null;
  etag: string;
  last_modified: number;
}): File {
  return {
    inodeId: row.inode_id,
    contentLength: row.content_length,
    contentType: row.content_type,
    etag: row.etag,
    lastModified: row.last_modified,
  };
}

function getFile(sql: SqlStorage, key: string): File | null {
  const row = sql
    .exec<{
      inode_id: number;
      content_length: number;
      content_type: string | null;
      etag: string;
      last_modified: number;
    }>(
      "SELECT i.id AS inode_id, i.content_length, i.content_type, i.etag, i.last_modified FROM webdav_paths AS p JOIN webdav_inodes AS i ON i.id = p.inode_id WHERE p.path = ?",
      key,
    )
    .toArray()
    .at(0);
  return row ? fileFrom(row) : null;
}

function readFile(sql: SqlStorage, inodeId: number): ReadableStream<Uint8Array> {
  const iterator = sql
    .exec<{ body: ArrayBuffer }>(
      "SELECT body FROM webdav_chunks WHERE inode_id = ? ORDER BY chunk_index",
      inodeId,
    )
    [Symbol.iterator]();
  return new ReadableStream({
    pull(controller) {
      const next = iterator.next();
      if (next.done) controller.close();
      else controller.enqueue(new Uint8Array(next.value.body));
    },
    cancel() {
      iterator.return?.();
    },
  });
}

function collectionExists(sql: SqlStorage, key: string): boolean {
  return (
    sql
      .exec<{ has_children: number }>(
        "SELECT EXISTS(SELECT 1 FROM webdav_paths WHERE path LIKE ? ESCAPE '\\') AS has_children",
        likePrefix(collectionPrefix(key)),
      )
      .one().has_children === 1
  );
}

function resource(sql: SqlStorage, key: string): Resource | null {
  if (key.length === 0) return { kind: "collection" };
  const file = getFile(sql, key);
  if (file) return { kind: "file", file };
  return collectionExists(sql, key) ? { kind: "collection" } : null;
}

function deleteFile(sql: SqlStorage, key: string): void {
  sql.exec("DELETE FROM webdav_paths WHERE path = ?", key);
  deleteOrphanedInodes(sql);
}

function deleteOrphanedInodes(sql: SqlStorage): void {
  sql.exec("DELETE FROM webdav_chunks WHERE inode_id NOT IN (SELECT inode_id FROM webdav_paths)");
  sql.exec("DELETE FROM webdav_inodes WHERE id NOT IN (SELECT inode_id FROM webdav_paths)");
}

function deleteCollection(sql: SqlStorage, key: string): void {
  const paths = likePrefix(collectionPrefix(key));
  sql.exec("DELETE FROM webdav_paths WHERE path LIKE ? ESCAPE '\\'", paths);
  deleteOrphanedInodes(sql);
}

function href(request: Request, requestedKey: string, key: string, collection: boolean): string {
  const url = new URL(request.url);
  const currentPath = url.pathname.replace(/\/$/, "");
  if (key === requestedKey) return `${currentPath || "/"}${collection && currentPath ? "/" : ""}`;
  const relativeKey = key.slice(requestedKey.length).replace(/^\//, "");
  const encodedKey = relativeKey.split("/").map(encodeURIComponent).join("/");
  return `${currentPath}/${encodedKey}${collection ? "/" : ""}`.replace(/\/\/+/g, "/");
}

function propResponse(
  request: Request,
  requestedKey: string,
  key: string,
  value: Resource,
): string {
  const isCollection = value.kind === "collection";
  const file = value.kind === "file" ? value.file : undefined;
  const displayName = key.split("/").at(-1) ?? "";
  const properties = [
    `<d:resourcetype>${isCollection ? "<d:collection/>" : ""}</d:resourcetype>`,
    xml`<d:displayname>${displayName}</d:displayname>`,
    file
      ? `<d:getlastmodified>${new Date(file.lastModified).toUTCString()}</d:getlastmodified>`
      : "",
    file ? `<d:getcontentlength>${file.contentLength}</d:getcontentlength>` : "",
    file?.contentType ? xml`<d:getcontenttype>${file.contentType}</d:getcontenttype>` : "",
    file ? xml`<d:getetag>${file.etag}</d:getetag>` : "",
  ].join("");
  return `<d:response><d:href>${xml`${href(request, requestedKey, key, isCollection)}`}</d:href><d:propstat><d:prop>${properties}</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>`;
}

function collectionChildren(sql: SqlStorage, key: string): Array<[string, Resource]> {
  const prefix = key.length === 0 ? "" : collectionPrefix(key);
  const rows = sql
    .exec<{
      inode_id: number;
      path: string;
      content_length: number;
      content_type: string | null;
      etag: string;
      last_modified: number;
    }>(
      "SELECT p.path, i.id AS inode_id, i.content_length, i.content_type, i.etag, i.last_modified FROM webdav_paths AS p JOIN webdav_inodes AS i ON i.id = p.inode_id WHERE p.path LIKE ? ESCAPE '\\' ORDER BY p.path",
      likePrefix(prefix),
    )
    .toArray();
  const children = new Map<string, Resource>();
  for (const row of rows) {
    const relativeKey = row.path.slice(prefix.length);
    const slash = relativeKey.indexOf("/");
    if (slash === -1) children.set(row.path, { kind: "file", file: fileFrom(row) });
    else children.set(`${prefix}${relativeKey.slice(0, slash)}`, { kind: "collection" });
  }
  return [...children.entries()];
}

function destinationKey(request: Request): string | null {
  const destination = request.headers.get("Destination");
  if (!destination) return null;
  try {
    const source = new URL(request.url);
    const target = new URL(destination, source);
    if (target.origin !== source.origin) return null;
    return keyFromPathname(target.pathname);
  } catch {
    return null;
  }
}

function createInode(storage: DurableObjectStorage): number {
  storage.sql.exec(
    "INSERT INTO webdav_inodes (content_length, content_type, etag, last_modified) VALUES (0, NULL, '', 0)",
  );
  return storage.sql.exec<{ id: number }>("SELECT last_insert_rowid() AS id").one().id;
}

function finishWrite(storage: DurableObjectStorage, key: string, file: File): void {
  storage.sql.exec(
    "UPDATE webdav_inodes SET content_length = ?, content_type = ?, etag = ?, last_modified = ? WHERE id = ?",
    file.contentLength,
    file.contentType,
    file.etag,
    file.lastModified,
    file.inodeId,
  );
  storage.sql.exec("INSERT INTO webdav_paths (path, inode_id) VALUES (?, ?)", key, file.inodeId);
}

async function writeChunks(
  storage: DurableObjectStorage,
  inodeId: number,
  body: ReadableStream<Uint8Array> | null,
): Promise<number> {
  const reader = body?.getReader();
  const buffer = new Uint8Array(CHUNK_SIZE);
  let buffered = 0;
  let chunkIndex = 0;
  let contentLength = 0;
  const writeChunk = (chunk: ArrayBuffer) => {
    storage.sql.exec(
      "INSERT INTO webdav_chunks (inode_id, chunk_index, body) VALUES (?, ?, ?)",
      inodeId,
      chunkIndex++,
      chunk,
    );
  };
  try {
    while (reader) {
      const next = await reader.read();
      if (next.done) break;
      for (let offset = 0; offset < next.value.byteLength;) {
        const length = Math.min(CHUNK_SIZE - buffered, next.value.byteLength - offset);
        buffer.set(next.value.subarray(offset, offset + length), buffered);
        buffered += length;
        offset += length;
        contentLength += length;
        if (buffered === CHUNK_SIZE) {
          writeChunk(buffer.buffer.slice(0));
          buffered = 0;
        }
      }
    }
    if (buffered > 0) {
      writeChunk(buffer.buffer.slice(0, buffered));
    }
    return contentLength;
  } catch (error) {
    storage.sql.exec("DELETE FROM webdav_chunks WHERE inode_id = ?", inodeId);
    storage.sql.exec("DELETE FROM webdav_inodes WHERE id = ?", inodeId);
    throw error;
  } finally {
    reader?.releaseLock();
  }
}

/** Creates a WebDAV application backed by Durable Object SQLite tables. */
export function webdav(storage: DurableObjectStorage) {
  const sql = storage.sql;
  migrate(storage);

  const app = new Hono<{ Variables: { key: string } }>();

  app.use("*", async (c, next) => {
    const key = keyFromPathname(c.req.path);
    if (key === null) return c.text("Invalid path", 400);
    c.set("key", key);
    await next();
  });

  app.options("*", () => new Response(null, { headers: DAV_HEADERS }));

  app.get("*", (c) => {
    const file = getFile(sql, c.get("key"));
    if (!file) return c.text("Not found", 404);
    const headers = new Headers({
      ETag: file.etag,
      "Content-Length": String(file.contentLength),
    });
    if (file.contentType) headers.set("Content-Type", file.contentType);
    return new Response(readFile(sql, file.inodeId), { headers });
  });

  app.on("HEAD", "*", (c) => {
    const file = getFile(sql, c.get("key"));
    if (!file) return c.text("Not found", 404);
    const headers = new Headers({ ETag: file.etag, "Content-Length": String(file.contentLength) });
    if (file.contentType) headers.set("Content-Type", file.contentType);
    return new Response(null, { headers });
  });

  app.put("*", async (c) => {
    const key = c.get("key");
    if (key.length === 0 || c.req.path.endsWith("/"))
      return c.text("Cannot overwrite a collection", 409);
    const existing = getFile(sql, key);
    deleteFile(sql, key);
    const inodeId = createInode(storage);
    const contentLength = await writeChunks(storage, inodeId, c.req.raw.body);
    const file: File = {
      inodeId,
      contentLength,
      contentType: c.req.header("Content-Type") ?? null,
      etag: `"${crypto.randomUUID()}"`,
      lastModified: Date.now(),
    };
    finishWrite(storage, key, file);
    return new Response(null, { status: existing ? 204 : 201, headers: { ETag: file.etag } });
  });

  app.delete("*", (c) => {
    const key = c.get("key");
    storage.transactionSync(() => {
      if (c.req.path.endsWith("/")) deleteCollection(sql, key);
      else deleteFile(sql, key);
    });
    return new Response(null, { status: 204 });
  });

  app.on("MKCOL", "*", () => new Response(null, { status: 405 }));

  app.on("PROPFIND", "*", async (c) => {
    await c.req.raw.arrayBuffer();
    const key = c.get("key");
    const selected = resource(sql, key);
    if (!selected) return new Response(null, { status: 404 });
    const responses = [propResponse(c.req.raw, key, key, selected)];
    if (selected.kind === "collection" && c.req.header("Depth") !== "0") {
      for (const [childKey, child] of collectionChildren(sql, key)) {
        responses.push(propResponse(c.req.raw, key, childKey, child));
      }
    }
    return c.body(
      `<?xml version="1.0" encoding="utf-8"?><d:multistatus xmlns:d="DAV:">${responses.join("")}</d:multistatus>`,
      207,
      { "Content-Type": "application/xml; charset=utf-8" },
    );
  });

  app.on(["COPY", "MOVE"], "*", (c) => {
    const key = c.get("key");
    const source = resource(sql, key);
    const targetKey = destinationKey(c.req.raw);
    if (!source) return c.text("Not found", 404);
    if (targetKey === null || targetKey.length === 0 || targetKey === key)
      return c.text("Invalid destination", 400);
    const destination = resource(sql, targetKey);
    if (destination && c.req.header("Overwrite")?.toUpperCase() === "F")
      return c.text("Destination exists", 412);
    storage.transactionSync(() => {
      if (destination) {
        if (destination.kind === "collection") deleteCollection(sql, targetKey);
        else deleteFile(sql, targetKey);
      }
      if (source.kind === "file") {
        sql.exec(
          "INSERT INTO webdav_paths (path, inode_id) VALUES (?, ?)",
          targetKey,
          source.file.inodeId,
        );
        if (c.req.method === "MOVE") deleteFile(sql, key);
      } else {
        const sourcePrefix = collectionPrefix(key);
        const targetPrefix = collectionPrefix(targetKey);
        const start = sourcePrefix.length + 1;
        const paths = likePrefix(sourcePrefix);
        sql.exec(
          "INSERT INTO webdav_paths SELECT ? || substr(path, ?), inode_id FROM webdav_paths WHERE path LIKE ? ESCAPE '\\'",
          targetPrefix,
          start,
          paths,
        );
        if (c.req.method === "MOVE") deleteCollection(sql, key);
      }
    });
    return new Response(null, { status: destination ? 204 : 201 });
  });

  app.all("*", () => new Response(null, { status: 405, headers: DAV_HEADERS }));
  return app;
}
