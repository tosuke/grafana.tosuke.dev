import { env } from "cloudflare:workers";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { webdav } from "./webdav";

const bucket = env.GRAFANA_LITESTREAM_BUCKET;
const app = webdav(bucket);
const prefix = "webdav-test/";

async function request(path: string, init?: RequestInit): Promise<Response> {
  return await app.fetch(new Request(`https://webdav.example${path}`, init));
}

beforeEach(async () => {
  await bucket.put(prefix, "");
});

afterEach(async () => {
  let cursor: string | undefined;
  do {
    const page = await bucket.list(cursor ? { prefix, cursor } : { prefix });
    if (page.objects.length > 0) await bucket.delete(page.objects.map((object) => object.key));
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
});

describe("R2 WebDAV", () => {
  it("answers Litestream's WebDAV capability probe", async () => {
    const response = await request(`/${prefix}`, { method: "OPTIONS" });

    expect(response.status).toBe(200);
    expect(response.headers.get("DAV")).toBe("1");
    expect(response.headers.get("Allow")).toContain("PROPFIND");
  });

  it("reports a missing backup directory as not found", async () => {
    const response = await request(`/${prefix}ltx/9/`, { method: "PROPFIND" });

    expect(response.status).toBe(404);
    // gowebdav does not consume error bodies here. Keep this empty so the
    // outbound tunnel remains usable for Litestream's following request.
    await expect(response.text()).resolves.toBe("");
  });

  it("stores files and exposes their WebDAV properties", async () => {
    const put = await request(`/${prefix}hello.txt`, {
      method: "PUT",
      headers: { "Content-Type": "text/plain" },
      body: "hello from R2",
    });
    expect(put.status).toBe(201);

    const get = await request(`/${prefix}hello.txt`);
    expect(get.status).toBe(200);
    expect(get.headers.get("Content-Type")).toBe("text/plain");
    await expect(get.text()).resolves.toBe("hello from R2");

    const propfind = await request(`/${prefix}`, { method: "PROPFIND", headers: { Depth: "1" } });
    expect(propfind.status).toBe(207);
    const body = await propfind.text();
    const firstResponse = body.match(/<d:response>.*?<\/d:response>/)?.[0];
    expect(firstResponse).toContain(`<d:href>/${prefix}</d:href>`);
    expect(firstResponse).toContain("<d:resourcetype><d:collection/></d:resourcetype>");
    expect(body).toContain(`<d:href>/${prefix}hello.txt</d:href>`);
    expect(body).toContain("<d:getcontentlength>13</d:getcontentlength>");
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

  it("rejects a PUT to a collection path without looking up its prefix", async () => {
    const response = await request(`/${prefix}collection/`, { method: "PUT", body: "contents" });

    expect(response.status).toBe(409);
  });

  it("treats MKCOL as a no-op for R2's implicit collections", async () => {
    const mkcol = await request(`/${prefix}source`, { method: "MKCOL" });
    // gowebdav treats 405 as success. Returning it without consulting R2 avoids
    // a head/list/put sequence for every LTX file Litestream uploads.
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
