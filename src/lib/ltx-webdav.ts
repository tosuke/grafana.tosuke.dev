import { env, RpcTarget } from "cloudflare:workers";
import { XMLParser } from "fast-xml-parser";
import { Hono } from "hono";
import { cache } from "hono/cache";
import type { Context } from "hono";
import * as z from "zod";

export const webDAVMethodHeader = "X-Method";
const txIDPattern = "[0-9a-f]".repeat(16);
const ltxLevelPath = "/ltx/:level{[0-9]}" as const;
const ltxFilePath = `/ltx/:level{[0-9]+}/:file{${txIDPattern}-${txIDPattern}\\.ltx}` as const;
const ltxCacheName = "ltx";

export function decodeWebDAVMethod(request: Request): Request {
  const encodedMethod = request.headers.get(webDAVMethodHeader);
  if (encodedMethod && ["MKCOL", "PROPFIND"].includes(encodedMethod) && request.method === "POST") {
    const headers = new Headers(request.headers);
    headers.delete(webDAVMethodHeader);
    return new Request(request, { method: encodedMethod, headers });
  }
  return request;
}

export function webdavApp(doStore: () => Rpc.Stub<DOLTXStore> | Rpc.Result<DOLTXStore>): Hono {
  const app = new Hono();

  const r2Store = new R2LTXStore();
  const store = (level: number): LTXFileStore => (level <= 1 ? doStore() : r2Store);

  app.on(
    "MKCOL",
    "*",
    () =>
      new Response(null, {
        status: 201,
      }),
  );

  app.on("PROPFIND", "/", (c) => c.body(null, 501));

  app.on("PROPFIND", ["/ltx", "/ltx/"], (c) => c.body(null, 501));

  app.on("PROPFIND", [ltxLevelPath, `${ltxLevelPath}/`], async (c) => {
    const level = Number(c.req.param("level"));
    const propfind = parsePropfindBody(await c.req.text());
    if (!propfind) {
      return c.body(null, 400);
    }
    const depth = c.req.header("Depth") ?? "infinity";
    if (depth !== "0" && depth !== "1" && depth !== "infinity") {
      return c.body(null, 400);
    }

    const resource: ResourcePath = {
      kind: "level",
      href: `/ltx/${level}/`,
      level,
    };

    let files: readonly LTXFileMetadata[] = [];
    if (depth !== "0") {
      files = await store(level).listFiles(level);
    }

    const self = resourceToDAVResource(resource, files);
    if (!self) {
      return c.body(null, 404);
    }

    const resources = [self];
    if (depth !== "0" && self.collection) {
      resources.push(...getChildren(resource, files));
    }

    const body = renderMultiStatus(resources, propfind);
    return c.body(body, 207, {
      "Content-Type": "application/xml; charset=utf-8",
    });
  });

  app.on("PROPFIND", ltxFilePath, (c) => c.body(null, 501));

  app.on(["GET", "HEAD"],
    ltxFilePath,
    cache({
      cacheName: ltxCacheName,
      cacheControl: "public, max-age=3600, immutable",
    }),
    async (c) => {
      const resource = fileResource(c);
      const { level, minTXID, maxTXID } = resource;
      const resp = await store(level).getFile(level, minTXID, maxTXID);
      if (c.req.method === "HEAD") {
        return new Response(null, {
          status: resp.status,
          headers: resp.headers,
        });
      }
      return resp;
    },
  );

  app.put(ltxFilePath, async (c) => {
    const resource = fileResource(c);
    const { level, minTXID, maxTXID } = resource;
    const body = c.req.raw.body;
    if (!body) return c.body(null, 400);
    return store(level).putFile(level, minTXID, maxTXID, c.req.raw.headers, body);
  });

  app.delete(ltxFilePath, async (c) => {
    const resource = fileResource(c);
    const { level, minTXID, maxTXID } = resource;
    await store(level).deleteFile(level, minTXID, maxTXID);
    return c.body(null, 204);
  });

  app.delete("/ltx", async (c) => {
    await Promise.all([doStore().deleteAll(), r2Store.deleteAll()]);
    return c.body(null, 204);
  });

  app.on(["HEAD", "GET", "PUT", "DELETE"], "/ltx/*", (c) => c.notFound());

  app.on("PROPFIND", "*", async (c) => {
    const propfind = parsePropfindBody(await c.req.text());
    return c.body(null, propfind ? 404 : 400);
  });

  app.all("*", (c) => {
    return new Response(null, {
      status: c.req.method === "OPTIONS" ? 200 : 405,
      headers: {
        Allow: "OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, PROPFIND",
        DAV: "1",
      },
    });
  });

  return app;
}

interface LTXFileStore {
  getFile(level: number, minTXID: string, maxTXID: string): Promise<Response>;
  listFiles(level: number): Promise<LTXFileMetadata[]>;
  putFile(
    level: number,
    minTXID: string,
    maxTXID: string,
    headers: Headers,
    body: ReadableStream<Uint8Array>,
  ): Promise<Response>;
  deleteFile(level: number, minTXID: string, maxTXID: string): Promise<void>;
  deleteAll(): Promise<void>;
}

type LTXFileMetadata = {
  level: number;
  minTXID: string;
  maxTXID: string;
  size: number;
  etag: string;
  modifiedAt: number;
};

export class DOLTXStore extends RpcTarget implements LTXFileStore {
  #ctx: DurableObjectState;
  static #initialized = new WeakSet<DurableObjectState>();
  static MIGRATIONS: ReadonlyArray<{ version: number; up: (sql: SqlStorage) => void }> = [
    {
      version: 1,
      up: (sql) => {
        sql.exec(`
          CREATE TABLE ltx_files (
            level INTEGER NOT NULL,
            min_tx_id TEXT NOT NULL,
            max_tx_id TEXT NOT NULL,
            etag TEXT NOT NULL,
            content_length INTEGER NOT NULL,
            content_type TEXT NOT NULL,
            modified_at INTEGER NOT NULL,
            body BLOB,
            PRIMARY KEY (level, min_tx_id, max_tx_id)
          )
        `);
      },
    },
  ];

  constructor(ctx: DurableObjectState) {
    super();
    this.#ctx = ctx;
    if (DOLTXStore.#initialized.has(ctx)) return;
    DOLTXStore.#migrate(ctx);
    DOLTXStore.#initialized.add(ctx);
  }

  static #migrate(ctx: DurableObjectState) {
    const sql = ctx.storage.sql;
    sql.exec(`
      CREATE TABLE IF NOT EXISTS ltx_migrations (
        version INTEGER PRIMARY KEY,
        applied_at INTEGER NOT NULL
      )
    `);
    const currentVersion =
      sql
        .exec<{ version: number }>("SELECT MAX(version) AS version FROM ltx_migrations")
        .toArray()
        .at(0)?.version ?? 0;
    const migrations = DOLTXStore.MIGRATIONS.toSorted((a, b) => a.version - b.version);
    const desiredVersion = migrations.at(-1)?.version ?? currentVersion;
    if (currentVersion < desiredVersion) {
      for (const migration of migrations) {
        if (migration.version <= currentVersion) continue;
        migration.up(sql);
      }
      sql.exec(
        "INSERT INTO ltx_migrations (version, applied_at) VALUES (?, ?)",
        desiredVersion,
        Date.now(),
      );
    }
  }

  async getFile(level: number, minTXID: string, maxTXID: string): Promise<Response> {
    const sql = this.#ctx.storage.sql;
    const row = sql
      .exec<{
        etag: string;
        content_length: number;
        content_type: string;
        body: ArrayBuffer | null;
      }>(
        "SELECT etag, content_length, content_type, body FROM ltx_files WHERE level = ? AND min_tx_id = ? AND max_tx_id = ?",
        level,
        minTXID,
        maxTXID,
      )
      .toArray()
      .at(0);
    if (!row) {
      return new Response(null, { status: 404 });
    }
    const headers = new Headers();
    headers.set("Content-Length", row.content_length.toString());
    headers.set("Content-Type", row.content_type);
    if (row.etag) {
      headers.set("ETag", `"${row.etag}"`);
    }
    if (row.body) {
      return new Response(row.body, { status: 200, headers });
    }
    const object = await env.GRAFANA_LTX_BUCKET.get(objectKey(level, minTXID, maxTXID));
    if (!object) {
      return new Response(null, { status: 404 });
    }
    return new Response(object.body, { status: 200, headers });
  }

  async listFiles(level: number): Promise<LTXFileMetadata[]> {
    return this.#ctx.storage.sql
      .exec<{
        level: number;
        min_tx_id: string;
        max_tx_id: string;
        etag: string;
        content_length: number;
        modified_at: number;
      }>(
        "SELECT level, min_tx_id, max_tx_id, etag, content_length, modified_at FROM ltx_files WHERE level = ? ORDER BY level, min_tx_id, max_tx_id",
        level,
      )
      .toArray()
      .map((row) => ({
        level: row.level,
        minTXID: row.min_tx_id,
        maxTXID: row.max_tx_id,
        size: row.content_length,
        etag: row.etag,
        modifiedAt: row.modified_at,
      }));
  }

  async putFile(
    level: number,
    minTXID: string,
    maxTXID: string,
    headers: Headers,
    bodyStream: ReadableStream<Uint8Array>,
  ): Promise<Response> {
    const contentLength = z.coerce
      .number()
      .int()
      .nonnegative()
      .safeParse(headers.get("Content-Length"));
    if (!contentLength.success) {
      return new Response(null, { status: 411 });
    }
    const contentType = headers.get("Content-Type") ?? "application/octet-stream";
    const etag = headers.get("ETag")?.replaceAll('"', "") ?? "";
    let body: ArrayBuffer | null = null;
    if (contentLength.data > 256 * 1024) {
      await env.GRAFANA_LTX_BUCKET.put(objectKey(level, minTXID, maxTXID), bodyStream, {
        httpMetadata: { contentType },
      });
    } else {
      body = await new Response(bodyStream).arrayBuffer();
    }
    const sql = this.#ctx.storage.sql;
    const exsits =
      sql
        .exec(
          "SELECT 1 FROM ltx_files WHERE level = ? AND min_tx_id = ? AND max_tx_id = ?",
          level,
          minTXID,
          maxTXID,
        )
        .toArray().length > 0;
    sql.exec(
      "INSERT OR REPLACE INTO ltx_files (level, min_tx_id, max_tx_id, etag, content_length, content_type, modified_at, body) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      level,
      minTXID,
      maxTXID,
      etag,
      contentLength.data,
      contentType,
      Date.now(),
      body,
    );
    return new Response(null, {
      status: exsits ? 204 : 201,
      headers: {
        ETag: `"${etag}"`,
      },
    });
  }

  async deleteFile(level: number, minTXID: string, maxTXID: string): Promise<void> {
    const sql = this.#ctx.storage.sql;
    sql.exec(
      "DELETE FROM ltx_files WHERE level = ? AND min_tx_id = ? AND max_tx_id = ?",
      level,
      minTXID,
      maxTXID,
    );
    await env.GRAFANA_LTX_BUCKET.delete(objectKey(level, minTXID, maxTXID));
  }

  async deleteAll(): Promise<void> {
    const sql = this.#ctx.storage.sql;
    sql.exec("DELETE FROM ltx_files");
  }
}

export class R2LTXStore implements LTXFileStore {
  get #bucket() {
    return env.GRAFANA_LTX_BUCKET;
  }

  async getFile(level: number, minTXID: string, maxTXID: string): Promise<Response> {
    const object = await this.#bucket.get(objectKey(level, minTXID, maxTXID));
    if (!object) {
      return new Response(null, { status: 404 });
    }
    const headers = new Headers();
    object.writeHttpMetadata(headers);
    headers.set("Content-Length", object.size.toString());
    headers.set("ETag", object.httpEtag);
    return new Response(object.body, { status: 200, headers });
  }

  async listFiles(level: number): Promise<LTXFileMetadata[]> {
    const objects: R2Object[] = [];
    const prefix = `ltx/${level}/`;
    let cursor: string | undefined;
    for (;;) {
      const page = await this.#bucket.list(cursor ? { prefix, cursor } : { prefix });
      objects.push(...page.objects);
      if (!page.truncated) break;
      cursor = page.cursor;
    }
    return objects.flatMap((object) => {
      const match = object.key.match(/^ltx\/(\d+)\/([0-9a-f]{16})-([0-9a-f]{16})\.ltx$/);
      if (!match) return [];
      const [, level, minTXID, maxTXID] = match;
      if (!level || !minTXID || !maxTXID) return [];
      return [
        {
          level: Number(level),
          minTXID,
          maxTXID,
          size: object.size,
          etag: object.etag,
          modifiedAt: object.uploaded.getTime(),
        },
      ];
    });
  }

  async putFile(
    level: number,
    minTXID: string,
    maxTXID: string,
    headers: Headers,
    body: ReadableStream<Uint8Array>,
  ): Promise<Response> {
    const contentLength = z.coerce
      .number()
      .int()
      .nonnegative()
      .safeParse(headers.get("Content-Length"));
    if (!contentLength.success) {
      return new Response(null, { status: 411 });
    }
    const bucket = this.#bucket;
    const key = objectKey(level, minTXID, maxTXID);
    const old = await bucket.head(key);
    const object = await bucket.put(key, body, {
      httpMetadata: { contentType: headers.get("Content-Type") ?? "application/octet-stream" },
    });
    return new Response(null, {
      status: old ? 204 : 201,
      headers: {
        ETag: object.httpEtag,
      },
    });
  }

  async deleteFile(level: number, minTXID: string, maxTXID: string): Promise<void> {
    await this.#bucket.delete(objectKey(level, minTXID, maxTXID));
  }

  async deleteAll(): Promise<void> {
    const bucket = this.#bucket;
    const { objects } = await bucket.list({ prefix: "ltx/" });
    await bucket.delete(objects.map((obj) => obj.key));
  }
}

function objectKey(level: number, minTXID: string, maxTXID: string): string {
  return `ltx/${level}/${minTXID}-${maxTXID}.ltx`;
}

type ResourcePath =
  | { kind: "root"; href: string }
  | { kind: "ltx"; href: string }
  | { kind: "level"; href: string; level: number }
  | { kind: "file"; href: string; level: number; minTXID: string; maxTXID: string };

type DAVResource = {
  href: string;
  name: string;
  collection: boolean;
  size: number;
  contentType: string;
  etag: string;
  modifiedAt: number;
};

type PropfindRequest =
  | { kind: "allprop" }
  | { kind: "propname" }
  | { kind: "prop"; names: readonly string[] };

function parsePropfindBody(body: string): PropfindRequest | null {
  try {
    if (!body.trim()) return { kind: "allprop" };

    const parsed: unknown = new XMLParser({
      ignoreAttributes: true,
      processEntities: false,
      removeNSPrefix: true,
      parseTagValue: false,
    }).parse(body);
    if (!isRecord(parsed) || !isRecord(parsed.propfind)) return null;

    const propfind = parsed.propfind;
    const modes = ["allprop", "propname", "prop"].filter((name) => name in propfind);
    if (modes.length !== 1) return null;

    const mode = modes[0];
    if (mode === "allprop") return { kind: "allprop" };
    if (mode === "propname") return { kind: "propname" };
    if (propfind.prop === "") return { kind: "prop", names: [] };
    if (!isRecord(propfind.prop)) return null;
    return { kind: "prop", names: Object.keys(propfind.prop) };
  } catch {
    return null;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function fileResource(c: Context<{}, typeof ltxFilePath>): Extract<ResourcePath, { kind: "file" }> {
  const { level, file } = c.req.param();
  const separator = file.indexOf("-");
  return {
    kind: "file",
    href: `/ltx/${level}/${encodeURIComponent(file)}`,
    level: Number(level),
    minTXID: file.slice(0, separator),
    maxTXID: file.slice(separator + 1, -".ltx".length),
  };
}

function resourceToDAVResource(
  resource: ResourcePath,
  files: readonly LTXFileMetadata[],
): DAVResource | null {
  switch (resource.kind) {
    case "root":
      return directoryResource(resource.href, "");
    case "ltx":
      return directoryResource(resource.href, "ltx");
    case "level":
      return directoryResource(resource.href, String(resource.level));
    case "file": {
      const file = files.find(
        (candidate) =>
          candidate.level === resource.level &&
          candidate.minTXID === resource.minTXID &&
          candidate.maxTXID === resource.maxTXID,
      );
      if (!file) return null;
      return {
        href: resource.href,
        name: `${resource.minTXID}-${resource.maxTXID}.ltx`,
        collection: false,
        size: file.size,
        contentType: "application/octet-stream",
        etag: file.etag,
        modifiedAt: file.modifiedAt,
      };
    }
  }
}

function getChildren(resource: ResourcePath, files: readonly LTXFileMetadata[]): DAVResource[] {
  switch (resource.kind) {
    case "root":
      return [directoryResource("/ltx/", "ltx")];
    case "ltx": {
      const levels = new Set(files.map((file) => file.level));
      return [...levels]
        .sort((a, b) => a - b)
        .map((level) => directoryResource(`/ltx/${level}/`, String(level)));
    }
    case "level":
      return files
        .filter((file) => file.level === resource.level)
        .map((file) => ({
          href: `/ltx/${file.level}/${file.minTXID}-${file.maxTXID}.ltx`,
          name: `${file.minTXID}-${file.maxTXID}.ltx`,
          collection: false,
          size: file.size,
          contentType: "application/octet-stream",
          etag: file.etag,
          modifiedAt: file.modifiedAt,
        }));
    case "file":
      return [];
  }
}

function directoryResource(href: string, name: string): DAVResource {
  return {
    href,
    name,
    collection: true,
    size: 0,
    contentType: "",
    etag: "",
    modifiedAt: 0,
  };
}

function renderMultiStatus(resources: readonly DAVResource[], propfind: PropfindRequest): string {
  const responses = resources
    .map((resource) => {
      const requested =
        propfind.kind === "allprop"
          ? DAV_PROPERTY_NAMES
          : propfind.kind === "propname"
            ? DAV_PROPERTY_NAMES
            : propfind.names;
      const supported = requested.filter((name): name is DAVPropertyName =>
        isDAVPropertyName(name),
      );
      const unsupported = requested.filter((name) => !isDAVPropertyName(name));
      const propstat = [];
      if (supported.length > 0) {
        propstat.push(renderPropstat(resource, supported, propfind.kind === "propname", 200));
      }
      if (unsupported.length > 0) {
        propstat.push(renderPropstat(resource, unsupported, true, 404));
      }
      return `<d:response><d:href>${escapeXML(resource.href)}</d:href>${propstat.join("")}</d:response>`;
    })
    .join("\n");
  return `<?xml version="1.0" encoding="utf-8"?><d:multistatus xmlns:d="DAV:">${responses}</d:multistatus>`;
}

const DAV_PROPERTY_NAMES = [
  "displayname",
  "resourcetype",
  "getcontentlength",
  "getcontenttype",
  "getetag",
  "getlastmodified",
] as const;

type DAVPropertyName = (typeof DAV_PROPERTY_NAMES)[number];

function isDAVPropertyName(name: string): name is DAVPropertyName {
  return (DAV_PROPERTY_NAMES as readonly string[]).includes(name);
}

function renderPropstat(
  resource: DAVResource,
  names: readonly string[],
  propname: boolean,
  status: number,
): string {
  const properties = names
    .map((name) => {
      if (propname || status !== 200) return `<d:${name}/>`;
      switch (name as DAVPropertyName) {
        case "displayname":
          return `<d:displayname>${escapeXML(resource.name)}</d:displayname>`;
        case "resourcetype":
          return `<d:resourcetype>${resource.collection ? "<d:collection/>" : ""}</d:resourcetype>`;
        case "getcontentlength":
          return `<d:getcontentlength>${resource.size}</d:getcontentlength>`;
        case "getcontenttype":
          return `<d:getcontenttype>${escapeXML(resource.contentType)}</d:getcontenttype>`;
        case "getetag":
          return `<d:getetag>${escapeXML(formatETag(resource.etag))}</d:getetag>`;
        case "getlastmodified":
          return `<d:getlastmodified>${new Date(resource.modifiedAt).toUTCString()}</d:getlastmodified>`;
      }
    })
    .join("");
  const statusText = status === 200 ? "200 OK" : "404 Not Found";
  return `<d:propstat><d:prop>${properties}</d:prop><d:status>HTTP/1.1 ${statusText}</d:status></d:propstat>`;
}

function formatETag(etag: string): string {
  if (!etag) return "";
  return etag.startsWith('"') ? etag : `"${etag}"`;
}

function escapeXML(value: string): string {
  return value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}
