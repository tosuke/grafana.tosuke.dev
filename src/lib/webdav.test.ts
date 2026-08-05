import { runInDurableObject } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { beforeEach, describe, expect, it } from "vitest";
import type { WebDavTest } from "./webdav-test-do";

const prefix = "webdav-test/";
let app: DurableObjectStub<WebDavTest>;

async function request(path: string, init?: RequestInit): Promise<Response> {
  return await app.fetch(new Request(`https://webdav.example${path}`, init));
}

beforeEach(() => {
  app = env.WEBDAV_TEST.get(env.WEBDAV_TEST.newUniqueId());
});

describe("Durable Object Storage WebDAV", () => {
  it("records applied schema migrations", async () => {
    await runInDurableObject(app, (_instance, state) => {
      const migrations = state.storage.sql
        .exec<{ version: number }>("SELECT version FROM webdav_migrations ORDER BY version")
        .toArray();

      expect(migrations).toEqual([{ version: 1 }]);
    });
  });

  it("answers Litestream's WebDAV capability probe", async () => {
    const response = await request(`/${prefix}`, { method: "OPTIONS" });

    expect(response.status).toBe(200);
    expect(response.headers.get("DAV")).toBe("1");
    expect(response.headers.get("Allow")).toContain("PROPFIND");
  });

  it("reports a missing backup directory as not found", async () => {
    const response = await request(`/${prefix}ltx/9/`, {
      method: "PROPFIND",
      body: '<?xml version="1.0"?><d:propfind xmlns:d="DAV:"/>',
    });

    expect(response.status).toBe(404);
    // gowebdav does not consume error bodies here. Keep this empty so the
    // outbound tunnel remains usable for Litestream's following request.
    await expect(response.text()).resolves.toBe("");
  });

  it("stores files and exposes their WebDAV properties", async () => {
    const put = await request(`/${prefix}hello.txt`, {
      method: "PUT",
      headers: { "Content-Type": "text/plain" },
      body: "hello from Durable Object Storage",
    });
    expect(put.status).toBe(201);

    const get = await request(`/${prefix}hello.txt`);
    expect(get.status).toBe(200);
    expect(get.headers.get("Content-Type")).toBe("text/plain");
    await expect(get.text()).resolves.toBe("hello from Durable Object Storage");

    const propfind = await request(`/${prefix}`, { method: "PROPFIND", headers: { Depth: "1" } });
    expect(propfind.status).toBe(207);
    const body = await propfind.text();
    const firstResponse = body.match(/<d:response>.*?<\/d:response>/)?.[0];
    expect(firstResponse).toContain(`<d:href>/${prefix}</d:href>`);
    expect(firstResponse).toContain("<d:resourcetype><d:collection/></d:resourcetype>");
    expect(body).toContain(`<d:href>/${prefix}hello.txt</d:href>`);
    expect(body).toContain("<d:getcontentlength>33</d:getcontentlength>");
    expect(body).toContain("<d:getlastmodified>");
  });

  it("returns 204 when replacing a file", async () => {
    expect((await request(`/${prefix}replace.txt`, { method: "PUT", body: "first" })).status).toBe(
      201,
    );
    expect((await request(`/${prefix}replace.txt`, { method: "PUT", body: "second" })).status).toBe(
      204,
    );
    await expect((await request(`/${prefix}replace.txt`)).text()).resolves.toBe("second");
  });

  it("splits files into 1.5 MiB chunks", async () => {
    const contentLength = 1.5 * 1024 * 1024 + 1;
    const path = `${prefix}large.bin`;
    let written = 0;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (written === contentLength) return controller.close();
        const chunk = new Uint8Array(Math.min(64 * 1024, contentLength - written));
        chunk.fill(42);
        written += chunk.byteLength;
        controller.enqueue(chunk);
      },
    });

    expect((await request(`/${path}`, { method: "PUT", body })).status).toBe(201);

    await runInDurableObject(app, (_instance, state) => {
      const chunks = state.storage.sql
        .exec<{ count: number; max_size: number }>(
          "SELECT COUNT(*) AS count, MAX(length(c.body)) AS max_size FROM webdav_paths AS p JOIN webdav_chunks AS c ON c.inode_id = p.inode_id WHERE p.path = ?",
          path,
        )
        .one();
      expect(chunks.count).toBe(2);
      expect(chunks.max_size).toBe(1.5 * 1024 * 1024);
    });
    const response = await request(`/${path}`);
    expect(response.headers.get("Content-Length")).toBe(String(contentLength));
    const reader = response.body?.getReader();
    let read = 0;
    while (reader) {
      const next = await reader.read();
      if (next.done) break;
      const chunk: Uint8Array = next.value;
      expect(chunk.every((byte) => byte === 42)).toBe(true);
      read += chunk.byteLength;
    }
    expect(read).toBe(contentLength);
  }, 15_000);

  it("rejects a PUT to a collection path", async () => {
    const response = await request(`/${prefix}collection/`, { method: "PUT", body: "contents" });

    expect(response.status).toBe(409);
  });

  it("treats MKCOL as a no-op for implicit collections", async () => {
    const mkcol = await request(`/${prefix}source`, { method: "MKCOL" });
    // gowebdav treats 405 as success, and a collection is inferred from the
    // files stored beneath its path.
    expect(mkcol.status).toBe(405);
    await expect(mkcol.text()).resolves.toBe("");

    expect(
      (await request(`/${prefix}source/nested/file.txt`, { method: "PUT", body: "contents" }))
        .status,
    ).toBe(201);

    expect(
      (
        await request(`/${prefix}source`, {
          method: "COPY",
          headers: { Destination: `https://webdav.example/${prefix}copy` },
        })
      ).status,
    ).toBe(201);
    await expect((await request(`/${prefix}copy/nested/file.txt`)).text()).resolves.toBe(
      "contents",
    );

    expect(
      (
        await request(`/${prefix}source`, {
          method: "MOVE",
          headers: { Destination: `https://webdav.example/${prefix}moved` },
        })
      ).status,
    ).toBe(201);
    expect((await request(`/${prefix}source`)).status).toBe(404);
    await expect((await request(`/${prefix}moved/nested/file.txt`)).text()).resolves.toBe(
      "contents",
    );

    expect((await request(`/${prefix}moved/`, { method: "DELETE" })).status).toBe(204);
    expect((await request(`/${prefix}moved/nested/file.txt`)).status).toBe(404);
  });
});
