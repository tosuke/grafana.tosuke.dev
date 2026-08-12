import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  createExecutionContext,
  runInDurableObject,
  waitOnExecutionContext,
} from "cloudflare:test";
import { env } from "cloudflare:workers";
import { XMLParser } from "fast-xml-parser";
import { decodeWebDAVMethod, webDAVMethodHeader, webdavApp } from "./ltx-webdav";
import type { Hono } from "hono";
import type { Test } from "./test-do";

type DAVPropstat = {
  prop: Record<string, unknown>;
  status: string;
};

type DAVResponse = {
  href: string;
  propstat: DAVPropstat[];
};

type MultiStatus = {
  multistatus: {
    response: DAVResponse[];
  };
};

const r2Keys = new Set<string>();

afterEach(async () => {
  if (r2Keys.size === 0) return;
  await env.GRAFANA_LTX_BUCKET.delete([...r2Keys]);
  r2Keys.clear();
});

function propfind(path: string, body?: string, depth = "0"): Request {
  return new Request(`https://replica.worker${path}`, {
    method: "PROPFIND",
    headers: { Depth: depth },
    ...(body === undefined ? {} : { body }),
  });
}

function parseMultiStatus(body: string): MultiStatus {
  return new XMLParser({
    removeNSPrefix: true,
    isArray: (name) => name === "response" || name === "propstat",
  }).parse(body) as MultiStatus;
}

function responseFor(body: MultiStatus, href: string): DAVResponse {
  const response = body.multistatus.response.find((item) => item.href === href);
  if (!response) throw new Error(`Missing DAV response for ${href}`);
  return response;
}

async function fetchWithExecutionContext(app: Hono, request: Request): Promise<Response> {
  const executionContext = createExecutionContext();
  const response = await app.fetch(request, {}, executionContext);
  await waitOnExecutionContext(executionContext);
  return response;
}

describe("LTX WebDAV", () => {
  let stub: DurableObjectStub<Test>;
  let app: Hono;

  beforeEach(async () => {
    stub = env.TEST.get(env.TEST.newUniqueId());
    const store = await stub.ltxStore;
    app = webdavApp(() => store);
    return () => {
      store[Symbol.dispose]();
    };
  });

  it.each([
    ["POST", "MKCOL", 201],
    ["POST", "PROPFIND", 207],
  ])("restores an encoded %s request as %s", async (carrierMethod, webDAVMethod, status) => {
    const encoded = new Request("https://replica.worker/ltx/0/", {
      method: carrierMethod,
      headers: {
        [webDAVMethodHeader]: webDAVMethod,
        Depth: "0",
      },
    });

    const decoded = decodeWebDAVMethod(encoded);
    expect(decoded.method).toBe(webDAVMethod);
    expect(decoded.headers.has(webDAVMethodHeader)).toBe(false);
    expect((await app.fetch(decoded)).status).toBe(status);
  });

  it("does not decode a method from the wrong carrier", () => {
    const request = new Request("https://replica.worker/", {
      method: "GET",
      headers: { [webDAVMethodHeader]: "PROPFIND" },
    });

    expect(decodeWebDAVMethod(request)).toBe(request);
  });

  it("treats an empty Litestream level as a virtual collection", async () => {
    const response = await app.fetch(propfind("/ltx/0/"));
    expect(response.status).toBe(207);
    const resource = responseFor(parseMultiStatus(await response.text()), "/ltx/0/");
    const allProperties = resource.propstat.find((item) => item.status === "HTTP/1.1 200 OK");
    expect(allProperties?.prop.resourcetype).toHaveProperty("collection");
  });

  it("consumes a PROPFIND body before returning an invalid-path response", async () => {
    let consumed = false;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        consumed = true;
        controller.enqueue(
          new TextEncoder().encode("<d:propfind xmlns:d='DAV:'><d:allprop/></d:propfind>"),
        );
        controller.close();
      },
    });
    const request = new Request("https://replica.worker/not-webdav", {
      method: "PROPFIND",
      body,
    });

    const response = await app.fetch(request);
    expect(response.status).toBe(404);
    expect(consumed).toBe(true);
    expect(await response.text()).toBe("");
  });

  it("accepts both slash forms for a collection", async () => {
    for (const path of ["/ltx/0", "/ltx/0/"]) {
      const response = await app.fetch(propfind(path));
      expect(response.status).toBe(207);
      expect(responseFor(parseMultiStatus(await response.text()), "/ltx/0/").href).toBe("/ltx/0/");
    }
  });

  it("parses a propfind body and reports unknown properties", async () => {
    const response = await app.fetch(
      propfind(
        "/ltx/0/",
        `<?xml version="1.0"?><d:propfind xmlns:d="DAV:" xmlns:x="urn:test"><d:prop><d:getetag/><x:unknown/></d:prop></d:propfind>`,
      ),
    );

    expect(response.status).toBe(207);
    const resource = responseFor(parseMultiStatus(await response.text()), "/ltx/0/");
    expect(
      resource.propstat.find((item) => item.status === "HTTP/1.1 200 OK")?.prop,
    ).toHaveProperty("getetag");
    expect(
      resource.propstat.find((item) => item.status === "HTTP/1.1 404 Not Found")?.prop,
    ).toHaveProperty("unknown");
    expect(resource.propstat.flatMap((item) => Object.keys(item.prop))).not.toContain(
      "displayname",
    );
  });

  it("persists low-level files in the LTX_TEST DO", async () => {
    const minTXID = "0000000000000003";
    const maxTXID = "0000000000000004";
    const path = `/ltx/0/${minTXID}-${maxTXID}.ltx`;
    const body = new Uint8Array([1, 2, 3]);

    const putResponse = await app.fetch(
      new Request(`https://replica.worker${path}`, {
        method: "PUT",
        headers: {
          "Content-Length": body.byteLength.toString(),
          "Content-Type": "application/octet-stream",
        },
        body,
      }),
    );
    expect(putResponse.status).toBe(201);
    await runInDurableObject(stub, (_instance, state) => {
      expect(
        state.storage.sql
          .exec<{
            level: number;
            min_tx_id: string;
            max_tx_id: string;
            content_length: number;
            content_type: string;
            body_size: number;
          }>(
            "SELECT level, min_tx_id, max_tx_id, content_length, content_type, length(body) AS body_size FROM ltx_files",
          )
          .toArray(),
      ).toEqual([
        {
          level: 0,
          min_tx_id: minTXID,
          max_tx_id: maxTXID,
          content_length: 3,
          content_type: "application/octet-stream",
          body_size: 3,
        },
      ]);
    });

    const getResponse = await fetchWithExecutionContext(
      app,
      new Request(`https://replica.worker${path}`, { method: "GET" }),
    );
    expect(getResponse.status).toBe(200);
    expect(new Uint8Array(await getResponse.arrayBuffer())).toEqual(body);

    const propfindResponse = await app.fetch(propfind("/ltx/0/", undefined, "1"));
    expect(propfindResponse.status).toBe(207);
    const listing = parseMultiStatus(await propfindResponse.text());
    expect(responseFor(listing, "/ltx/0/").href).toBe("/ltx/0/");
    const fileResource = responseFor(listing, path);
    expect(
      fileResource.propstat.find((item) => item.status === "HTTP/1.1 200 OK")?.prop,
    ).toMatchObject({
      getcontentlength: 3,
    });

    const deleteResponse = await app.fetch(
      new Request(`https://replica.worker${path}`, { method: "DELETE" }),
    );
    expect(deleteResponse.status).toBe(204);
    await runInDurableObject(stub, (_instance, state) => {
      expect(state.storage.sql.exec("SELECT 1 FROM ltx_files").toArray()).toEqual([]);
    });
  });

  it("persists high-level files in R2", async () => {
    const level = 2;
    const minTXID = "0000000000000005";
    const maxTXID = "0000000000000006";
    const path = `/ltx/${level}/${minTXID}-${maxTXID}.ltx`;
    const key = `ltx/${level}/${minTXID}-${maxTXID}.ltx`;
    const body = new Uint8Array([4, 5, 6]);
    r2Keys.add(key);

    const putResponse = await app.fetch(
      new Request(`https://replica.worker${path}`, {
        method: "PUT",
        headers: {
          "Content-Length": body.byteLength.toString(),
          "Content-Type": "application/octet-stream",
        },
        body,
      }),
    );
    expect(putResponse.status).toBe(201);
    await expect(env.GRAFANA_LTX_BUCKET.head(key)).resolves.toMatchObject({ size: 3 });

    const getResponse = await fetchWithExecutionContext(
      app,
      new Request(`https://replica.worker${path}`, { method: "GET" }),
    );
    expect(getResponse.status).toBe(200);
    expect(new Uint8Array(await getResponse.arrayBuffer())).toEqual(body);

    const deleteResponse = await app.fetch(
      new Request(`https://replica.worker${path}`, { method: "DELETE" }),
    );
    expect(deleteResponse.status).toBe(204);
    await expect(env.GRAFANA_LTX_BUCKET.head(key)).resolves.toBeNull();
  });
});
