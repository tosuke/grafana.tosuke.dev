import { runInDurableObject } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { beforeEach, describe, expect, it } from "vitest";
import type { LtxTest } from "./ltx-test-do";

const CHUNK_SIZE = 2 * 1024 * 1024;

type FileKey = {
  level: number;
  min_txid: string;
  max_txid: string;
};

type FileInfo = FileKey & {
  size: number;
  created_at: number;
};

type RPCResponse =
  | { type: "list"; files: FileInfo[] }
  | { type: "read" | "delete" | "delete-all" | "not-found" }
  | { type: "write"; file: FileInfo };

const file: FileKey = {
  level: 0,
  min_txid: "0000000000000001",
  max_txid: "0000000000000002",
};
const createdAt = 1_700_000_000;

let stub: DurableObjectStub<LtxTest>;

async function connect(): Promise<WebSocket> {
  const response = await stub.fetch(
    new Request("https://ltx.test", { headers: { Upgrade: "websocket" } }),
  );
  expect(response.status).toBe(101);
  if (!response.webSocket) throw new Error("WebSocket upgrade did not return a socket");
  response.webSocket.accept();
  return response.webSocket;
}

type SocketMessage = string | ArrayBuffer | ArrayBufferView | Blob;

function receive(socket: WebSocket): Promise<SocketMessage> {
  return new Promise((resolve, reject) => {
    const message = (event: MessageEvent) => {
      cleanup();
      resolve(event.data as SocketMessage);
    };
    const close = () => {
      cleanup();
      reject(new Error("WebSocket closed before receiving a message"));
    };
    const error = () => {
      cleanup();
      reject(new Error("WebSocket error"));
    };
    const cleanup = () => {
      socket.removeEventListener("message", message);
      socket.removeEventListener("close", close);
      socket.removeEventListener("error", error);
    };
    socket.addEventListener("message", message);
    socket.addEventListener("close", close);
    socket.addEventListener("error", error);
  });
}

async function receiveJSON(socket: WebSocket): Promise<RPCResponse> {
  const message = await receive(socket);
  if (typeof message !== "string") throw new Error("Expected a text WebSocket message");
  return JSON.parse(message) as RPCResponse;
}

async function writeFile(socket: WebSocket, chunks: Uint8Array[]): Promise<FileInfo> {
  socket.send(
    JSON.stringify({
      type: "write",
      write_file: { ...file, created_at: createdAt },
    }),
  );
  const response = receiveJSON(socket);
  for (const chunk of chunks) socket.send(chunk);
  if (chunks.at(-1)?.byteLength === CHUNK_SIZE) socket.send(new ArrayBuffer(0));
  const result = await response;
  if (result.type !== "write") throw new Error(`Unexpected response: ${result.type}`);
  return result.file;
}

async function listFiles(socket: WebSocket): Promise<FileInfo[]> {
  socket.send(JSON.stringify({ type: "list" }));
  const response = await receiveJSON(socket);
  if (response.type !== "list") throw new Error(`Unexpected response: ${response.type}`);
  return response.files;
}

async function readFile(socket: WebSocket, offset: number, size: number): Promise<Uint8Array> {
  socket.send(
    JSON.stringify({
      type: "read",
      read_file: { ...file, offset, size },
    }),
  );
  const response = await receiveJSON(socket);
  if (response.type === "not-found") return new Uint8Array();
  if (response.type !== "read") throw new Error(`Unexpected response: ${response.type}`);

  const chunks: Uint8Array[] = [];
  for (;;) {
    const message = await receive(socket);
    if (typeof message === "string") throw new Error("Expected a binary file chunk");
    const body = message instanceof Blob ? await message.arrayBuffer() : message;
    const chunk = new Uint8Array(body instanceof ArrayBuffer ? body : body.buffer);
    chunks.push(chunk);
    if (chunk.byteLength < CHUNK_SIZE) break;
  }
  return Uint8Array.from(chunks.flatMap((chunk) => [...chunk]));
}

beforeEach(() => {
  stub = env.LTX_TEST.get(env.LTX_TEST.newUniqueId());
});

describe("LTX WebSocket storage", () => {
  it("creates the LTX schema", async () => {
    const socket = await connect();
    await listFiles(socket);
    await runInDurableObject(stub, (_instance, state) => {
      const tables = state.storage.sql
        .exec<{ name: string }>(
          "SELECT name FROM sqlite_schema WHERE type = 'table' AND name LIKE 'ltx_%' ORDER BY name",
        )
        .toArray();
      expect(tables).toEqual([
        { name: "ltx_chunks" },
        { name: "ltx_file_id" },
        { name: "ltx_files" },
        { name: "ltx_migrations" },
      ]);
      expect(
        state.storage.sql
          .exec<{ version: number }>("SELECT version FROM ltx_migrations ORDER BY version")
          .toArray(),
      ).toEqual([{ version: 1 }, { version: 2 }]);
    });
  });

  it("publishes a completed upload in the file list", async () => {
    const socket = await connect();
    await expect(writeFile(socket, [new Uint8Array([1, 2, 3])])).resolves.toEqual({
      ...file,
      created_at: createdAt,
      size: 3,
    });
    await expect(listFiles(socket)).resolves.toEqual([{ ...file, created_at: createdAt, size: 3 }]);
  });

  it("stores files in 2 MiB chunks", async () => {
    const socket = await connect();
    await writeFile(socket, [
      new Uint8Array(CHUNK_SIZE).fill(5),
      new Uint8Array(256 * 1024).fill(6),
    ]);

    await runInDurableObject(stub, (_instance, state) => {
      expect(
        state.storage.sql
          .exec<{ chunk_index: number; size: number }>(
            "SELECT chunk_index, length(body) AS size FROM ltx_chunks ORDER BY chunk_index",
          )
          .toArray(),
      ).toEqual([
        { chunk_index: 0, size: CHUNK_SIZE },
        { chunk_index: 1, size: 256 * 1024 },
      ]);
    });
  });

  it("reads data within and across storage chunks", async () => {
    const socket = await connect();
    await writeFile(socket, [
      new Uint8Array(CHUNK_SIZE).fill(5),
      new Uint8Array(256 * 1024).fill(6),
    ]);

    await expect(readFile(socket, 0, 2)).resolves.toEqual(new Uint8Array([5, 5]));
    await expect(readFile(socket, 0, 0)).resolves.toEqual(
      Uint8Array.from([
        ...new Uint8Array(CHUNK_SIZE).fill(5),
        ...new Uint8Array(256 * 1024).fill(6),
      ]),
    );
    await expect(readFile(socket, CHUNK_SIZE - 2, 256 * 1024)).resolves.toEqual(
      Uint8Array.from([5, 5, ...new Uint8Array(256 * 1024 - 2).fill(6)]),
    );
    await expect(readFile(socket, CHUNK_SIZE + 256 * 1024 - 2, 2)).resolves.toEqual(
      new Uint8Array([6, 6]),
    );
  });

  it("returns not-found for missing files or reads past EOF", async () => {
    const socket = await connect();
    await expect(readFile(socket, 0, 1)).resolves.toEqual(new Uint8Array());
    await writeFile(socket, [new Uint8Array([1, 2, 3])]);
    await expect(readFile(socket, 2, 2)).resolves.toEqual(new Uint8Array());
  });

  it("deletes selected files and all files", async () => {
    const socket = await connect();
    await writeFile(socket, [new Uint8Array([1, 2, 3])]);
    await expect(listFiles(socket)).resolves.toHaveLength(1);

    socket.send(JSON.stringify({ type: "delete", delete_files: { files: [file] } }));
    await expect(receiveJSON(socket)).resolves.toEqual({ type: "delete" });
    await expect(listFiles(socket)).resolves.toEqual([]);

    await writeFile(socket, [new Uint8Array([4, 5, 6])]);
    socket.send(JSON.stringify({ type: "delete-all" }));
    await expect(receiveJSON(socket)).resolves.toEqual({ type: "delete-all" });
    await expect(listFiles(socket)).resolves.toEqual([]);
  });
});
