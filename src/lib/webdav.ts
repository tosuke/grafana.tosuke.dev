import { Hono } from "hono";

const DAV_HEADERS = {
  Allow: "OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, MOVE, COPY, PROPFIND",
  DAV: "1",
};

type Resource = { kind: "file"; object: R2Object } | { kind: "collection"; object?: R2Object };

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

function collectionMarker(key: string): string {
  return `${key}/`;
}

async function resource(bucket: R2Bucket, key: string): Promise<Resource | null> {
  if (key.length === 0) return { kind: "collection" };

  const object = await bucket.head(key);
  if (object) return { kind: "file", object };

  const marker = await bucket.head(collectionMarker(key));
  if (
    marker ||
    (await bucket.list({ prefix: collectionMarker(key), limit: 1 })).objects.length > 0
  ) {
    return marker ? { kind: "collection", object: marker } : { kind: "collection" };
  }
  return null;
}

async function deleteCollection(bucket: R2Bucket, key: string): Promise<void> {
  let cursor: string | undefined;
  do {
    const page = await bucket.list({
      prefix: collectionMarker(key),
      limit: 1000,
      ...(cursor ? { cursor } : {}),
    });
    if (page.objects.length > 0) await bucket.delete(page.objects.map((object) => object.key));
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
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
  const object = value.kind === "file" ? value.object : value.object;
  const displayName = key.split("/").at(-1) ?? "";
  const properties = [
    `<d:resourcetype>${isCollection ? "<d:collection/>" : ""}</d:resourcetype>`,
    xml`<d:displayname>${displayName}</d:displayname>`,
    object ? `<d:getlastmodified>${object.uploaded.toUTCString()}</d:getlastmodified>` : "",
    !isCollection && object ? `<d:getcontentlength>${object.size}</d:getcontentlength>` : "",
    !isCollection && object?.httpMetadata?.contentType
      ? xml`<d:getcontenttype>${object.httpMetadata.contentType}</d:getcontenttype>`
      : "",
    object ? xml`<d:getetag>${object.httpEtag}</d:getetag>` : "",
  ].join("");
  return `<d:response><d:href>${xml`${href(request, requestedKey, key, isCollection)}`}</d:href><d:propstat><d:prop>${properties}</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>`;
}

async function collectionChildren(
  bucket: R2Bucket,
  key: string,
): Promise<Array<[string, Resource]>> {
  const prefix = key.length === 0 ? "" : collectionMarker(key);
  const children: Array<[string, Resource]> = [];
  let cursor: string | undefined;
  do {
    const page = await bucket.list({
      prefix,
      delimiter: "/",
      limit: 1000,
      ...(cursor ? { cursor } : {}),
    });
    for (const object of page.objects) {
      if (object.key !== prefix) children.push([object.key, { kind: "file", object }]);
    }
    for (const childPrefix of page.delimitedPrefixes) {
      children.push([childPrefix.slice(0, -1), { kind: "collection" }]);
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
  return children;
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

/** Creates a WebDAV application backed by an R2 bucket. */
export function webdav(bucket: R2Bucket) {
  const app = new Hono<{ Variables: { key: string } }>();

  app.use("*", async (c, next) => {
    const key = keyFromPathname(c.req.path);
    if (key === null) return c.text("Invalid path", 400);
    c.set("key", key);
    await next();
  });

  app.options("*", () => {
    // Litestream's WebDAV client requires a successful OPTIONS probe to
    // return 200 (rather than the otherwise-valid 204 response).
    return new Response(null, { headers: DAV_HEADERS });
  });

  app.get("*", async (c) => {
    const key = c.get("key");
    const object = await bucket.get(key);
    if (!object) return c.text("Not found", 404);
    const headers = new Headers();
    object.writeHttpMetadata(headers);
    headers.set("ETag", object.httpEtag);
    headers.set("Content-Length", String(object.size));
    return new Response(object.body, { headers });
  });

  app.on("HEAD", "*", async (c) => {
    const key = c.get("key");
    const object = await bucket.head(key);
    if (!object) return c.text("Not found", 404);
    const headers = new Headers();
    object.writeHttpMetadata(headers);
    headers.set("ETag", object.httpEtag);
    headers.set("Content-Length", String(object.size));
    return new Response(null, { headers });
  });

  app.put("*", async (c) => {
    const key = c.get("key");
    // This WebDAV namespace reserves paths ending in "/" for collections.
    // R2 prefixes are implicit, so checking whether an un-suffixed key has
    // descendants would only add reads without changing Litestream's usage.
    if (key.length === 0 || c.req.path.endsWith("/"))
      return c.text("Cannot overwrite a collection", 409);
    const existing = await bucket.head(key);
    const headers = c.req.raw.headers;
    const object = await bucket.put(key, c.req.raw.body, { httpMetadata: headers });
    return new Response(null, {
      status: existing ? 204 : 201,
      headers: { ETag: object.httpEtag },
    });
  });

  app.delete("*", async (c) => {
    const key = c.get("key");
    // R2 delete operations are idempotent. As with PUT, a trailing slash
    // selects the implicit collection namespace without a prefix probe.
    if (c.req.path.endsWith("/")) await deleteCollection(bucket, key);
    else await bucket.delete(key);
    return new Response(null, { status: 204 });
  });

  app.on("MKCOL", "*", () => {
    // R2 has implicit collections: a prefix needs no marker object before
    // PUT creates an object beneath it. gowebdav treats 405 as success for
    // an existing collection, so avoid R2 reads and marker writes here.
    return new Response(null, { status: 405 });
  });

  app.on("PROPFIND", "*", async (c) => {
    const key = c.get("key");
    if (key.length === 0 || c.req.path.endsWith("/")) {
      const children = await collectionChildren(bucket, key);
      if (key.length > 0 && children.length === 0) return new Response(null, { status: 404 });
      const responses = [propResponse(c.req.raw, key, key, { kind: "collection" })];
      if (c.req.header("Depth") !== "0") {
        for (const [childKey, child] of children) {
          responses.push(propResponse(c.req.raw, key, childKey, child));
        }
      }
      return c.body(
        `<?xml version="1.0" encoding="utf-8"?><d:multistatus xmlns:d="DAV:">${responses.join("")}</d:multistatus>`,
        207,
        { "Content-Type": "application/xml; charset=utf-8" },
      );
    }

    const object = await bucket.head(key);
    if (!object) return new Response(null, { status: 404 });
    const responses = [propResponse(c.req.raw, key, key, { kind: "file", object })];
    return c.body(
      `<?xml version="1.0" encoding="utf-8"?><d:multistatus xmlns:d="DAV:">${responses.join("")}</d:multistatus>`,
      207,
      { "Content-Type": "application/xml; charset=utf-8" },
    );
  });

  app.on(["COPY", "MOVE"], "*", async (c) => {
    const key = c.get("key");
    const source = await resource(bucket, key);
    const targetKey = destinationKey(c.req.raw);
    if (!source) return c.text("Not found", 404);
    if (targetKey === null || targetKey.length === 0 || targetKey === key)
      return c.text("Invalid destination", 400);
    const destination = await resource(bucket, targetKey);
    if (destination && c.req.header("Overwrite")?.toUpperCase() === "F")
      return c.text("Destination exists", 412);
    if (destination) {
      if (destination.kind === "collection") await deleteCollection(bucket, targetKey);
      else await bucket.delete(targetKey);
    }
    if (source.kind === "file") {
      const body = await bucket.get(key);
      if (!body) return c.text("Not found", 404);
      await bucket.put(
        targetKey,
        body.body,
        body.httpMetadata ? { httpMetadata: body.httpMetadata } : undefined,
      );
      if (c.req.method === "MOVE") await bucket.delete(key);
    } else {
      const prefix = collectionMarker(key);
      let cursor: string | undefined;
      do {
        const page = await bucket.list(
          cursor ? { prefix, cursor, limit: 1000 } : { prefix, limit: 1000 },
        );
        await Promise.all(
          page.objects.map((object) =>
            bucket
              .get(object.key)
              .then(
                (body) =>
                  body &&
                  bucket.put(
                    `${collectionMarker(targetKey)}${object.key.slice(prefix.length)}`,
                    body.body,
                    body.httpMetadata ? { httpMetadata: body.httpMetadata } : undefined,
                  ),
              ),
          ),
        );
        if (c.req.method === "MOVE" && page.objects.length > 0)
          await bucket.delete(page.objects.map((object) => object.key));
        cursor = page.truncated ? page.cursor : undefined;
      } while (cursor);
    }
    return new Response(null, { status: destination ? 204 : 201 });
  });

  app.all("*", () => {
    return new Response(null, { status: 405, headers: DAV_HEADERS });
  });

  return app;
}
