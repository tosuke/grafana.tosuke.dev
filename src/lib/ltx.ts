import { tracing } from "cloudflare:workers";
import * as z from "zod";

const LTX_TAG = "ltx";
const LTX_CHUNK_SIZE = 2 * 1024 * 1024; // 2 MiB

const RPCRequestSchema = z.discriminatedUnion("type", [
  z.object({
    type: z.literal("list"),
  }),
  z.object({
    type: z.literal("read"),
    read_file: z.object({
      level: z.number(),
      min_txid: z.string(),
      max_txid: z.string(),
      offset: z.number(),
      size: z.number(),
    }),
  }),
  z.object({
    type: z.literal("write"),
    write_file: z.object({
      level: z.number(),
      min_txid: z.string(),
      max_txid: z.string(),
      created_at: z.number(),
    }),
  }),
  z.object({
    type: z.literal("delete"),
    delete_files: z.object({
      files: z.array(
        z.object({
          level: z.number(),
          min_txid: z.string(),
          max_txid: z.string(),
        }),
      ),
    }),
  }),
  z.object({
    type: z.literal("delete-all"),
  }),
]);

type RPCRequest = z.infer<typeof RPCRequestSchema>;

type RPCResponse =
  | {
      readonly type: "list";
      readonly files: ReadonlyArray<{
        level: number;
        min_txid: string;
        max_txid: string;
        size: number;
        created_at: number;
      }>;
    }
  | {
      readonly type: "read" | "delete" | "delete-all" | "not-found";
    }
  | {
      readonly type: "write";
      readonly file: {
        level: number;
        min_txid: string;
        max_txid: string;
        size: number;
        created_at: number;
      };
    };

type WSAttachment = {
  id: string;
  state: "waiting" | "on-request" | "on-response";
  writingFileID?: number;
};

export class LTXStorage {
  #ctx: DurableObjectState;
  #tasks = new Map<string, Generator<void, RPCResponse, ArrayBuffer>>();

  constructor(ctx: DurableObjectState) {
    this.#ctx = ctx;
    migrate(ctx.storage);
  }

  accept(): WebSocket {
    const { 0: client, 1: server } = new WebSocketPair();

    const attachment: WSAttachment = {
      id: crypto.randomUUID(),
      state: "waiting",
    };
    server.serializeAttachment(attachment);
    this.#ctx.acceptWebSocket(server, [LTX_TAG]);

    return client;
  }

  async handleMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    if (ws.readyState !== WebSocket.READY_STATE_OPEN || !this.#ctx.getTags(ws).includes(LTX_TAG))
      return;
    const attachment: WSAttachment = ws.deserializeAttachment();

    try {
      if (typeof message !== "string") {
        if (attachment.state !== "on-request") {
          console.error("Invalid socket state", { attachment });
          ws.close(1003, "Invalid message type");
          return;
        }

        const task = this.#tasks.get(attachment.id);
        if (!task) {
          console.error("No task found for attachment", { attachment });
          ws.close(1003, "Invalid message type");
          return;
        }

        const result = task.next(message);
        if (!result.done) return;
        this.#tasks.delete(attachment.id);

        ws.send(JSON.stringify(result.value));
        ws.serializeAttachment({
          id: attachment.id,
          state: "waiting",
        });
        return;
      }
      if (attachment.state !== "waiting") {
        console.error("Invalid socket state", { attachment });
        ws.close(1003, "Invalid message type");
        return;
      }
      const request = RPCRequestSchema.parse(JSON.parse(message));
      switch (request.type) {
        case "list":
          tracing.enterSpan("ltx.list", () => {
            ws.send(JSON.stringify(listFiles(this.#ctx.storage.sql)));
          });
          break;
        case "read":
          await tracing.enterSpan("ltx.read", (span) =>
            readFile(span, ws, attachment, this.#ctx.storage.sql, request),
          );
          break;
        case "write":
          const task = tracing.startActiveSpan("ltx.write", (span) =>
            writeFile(span, ws, attachment, this.#ctx.storage, request),
          );
          task.next();
          this.#tasks.set(attachment.id, task);
          break;
        case "delete":
          tracing.enterSpan("ltx.delete", () => {
            this.#ctx.storage.transactionSync(() => {
              for (const file of request.delete_files.files) {
                const selected = this.#ctx.storage.sql
                  .exec<{ id: number }>(
                    "SELECT id FROM ltx_files WHERE level = ? AND min_tx_id = ? AND max_tx_id = ?",
                    file.level,
                    file.min_txid,
                    file.max_txid,
                  )
                  .toArray()
                  .at(0);
                if (!selected) continue;
                this.#ctx.storage.sql.exec("DELETE FROM ltx_chunks WHERE file_id = ?", selected.id);
                this.#ctx.storage.sql.exec("DELETE FROM ltx_files WHERE id = ?", selected.id);
              }
            });
            ws.send(JSON.stringify({ type: "delete" }));
          });
          break;
        case "delete-all":
          tracing.enterSpan("ltx.delete-all", () => {
            this.#ctx.storage.transactionSync(() => {
              this.#ctx.storage.sql.exec("DELETE FROM ltx_chunks");
              this.#ctx.storage.sql.exec("DELETE FROM ltx_files");
            });
            ws.send(JSON.stringify({ type: "delete-all" }));
          });
          break;
      }
    } catch (err) {
      console.error("Failed to handle message", { err, attachment });
      ws.close(1011, "Internal server error");
    }
  }

  handleClose(ws: WebSocket): void {
    if (!this.#ctx.getTags(ws).includes(LTX_TAG)) return;
    const attachment: WSAttachment = ws.deserializeAttachment();
    if (attachment.state === "on-request" && attachment.writingFileID) {
      this.#ctx.storage.sql.exec(
        "DELETE FROM ltx_chunks WHERE file_id = ?",
        attachment.writingFileID,
      );
    }
    try {
      this.#tasks.get(attachment.id)?.throw(new Error("WebSocket closed"));
    } catch {
      // Closing an in-progress upload is expected to abort its generator.
    }
    this.#tasks.delete(attachment.id);
  }
}

function listFiles(sql: SqlStorage): RPCResponse & { type: "list" } {
  const files = sql
    .exec<{
      level: number;
      min_txid: string;
      max_txid: string;
      size: number;
      created_at: number;
    }>(
      "SELECT level, min_tx_id AS min_txid, max_tx_id AS max_txid, size, created_at FROM ltx_files ORDER BY level, min_tx_id, max_tx_id",
    )
    .toArray();
  return {
    type: "list",
    files,
  };
}

async function readFile(
  span: Span,
  ws: WebSocket,
  attachment: WSAttachment,
  sql: SqlStorage,
  request: RPCRequest & { type: "read" },
): Promise<void> {
  const file = sql
    .exec<{
      id: number;
      size: number;
    }>(
      "SELECT id, size FROM ltx_files WHERE level = ? AND min_tx_id = ? AND max_tx_id = ?",
      request.read_file.level,
      request.read_file.min_txid,
      request.read_file.max_txid,
    )
    .toArray()
    .at(0);
  const requestedEnd = request.read_file.offset + request.read_file.size;
  if (!file || (request.read_file.size !== 0 && file.size < requestedEnd)) {
    ws.send(JSON.stringify({ type: "not-found" }));
    return;
  }
  ws.send(JSON.stringify({ type: "read" }));
  ws.serializeAttachment({
    ...attachment,
    state: "on-request",
  });
  span.setAttribute("file.id", file.id);
  span.setAttribute("file.level", request.read_file.level);
  span.setAttribute("file.min_txid", request.read_file.min_txid);
  span.setAttribute("file.max_txid", request.read_file.max_txid);
  span.setAttribute("file.size", file.size);
  span.setAttribute("read.offset", request.read_file.offset);
  span.setAttribute("read.size", request.read_file.size === 0 ? file.size : request.read_file.size);

  const begin = request.read_file.offset,
    end = request.read_file.size === 0 ? file.size : requestedEnd;
  const chunks = sql.exec<{ begin_offset: number; end_offset: number; body: ArrayBuffer }>(
    "SELECT begin_offset, end_offset, body FROM ltx_chunks WHERE file_id = ? AND begin_offset < ? AND end_offset > ? ORDER BY chunk_index ASC",
    file.id,
    end,
    begin,
  );
  const buf = new Uint8Array(LTX_CHUNK_SIZE);
  let bufLen = 0;
  for (const chunk of chunks) {
    let body = new Uint8Array(chunk.body);
    body = body.slice(
      Math.max(begin - chunk.begin_offset, 0),
      Math.min(end - chunk.begin_offset, body.byteLength),
    );
    if (bufLen + body.length < buf.length) {
      buf.set(body, bufLen);
      bufLen += body.length;
      continue;
    }

    if (bufLen == 0 && body.length === buf.length) {
      ws.send(body);
    } else {
      buf.set(body.slice(0, buf.length - bufLen), bufLen);
      ws.send(buf);
      buf.set(body.slice(buf.length - bufLen));
      bufLen = body.length - (buf.length - bufLen);
    }
    await scheduler.wait(10); // yield to event loop to send buffer
  }
  ws.send(buf.slice(0, bufLen)); // send remaining buffer
  ws.serializeAttachment({
    ...attachment,
    state: "waiting",
  });
}

function* writeFile(
  span: Span,
  ws: WebSocket,
  attachment: WSAttachment,
  storage: DurableObjectStorage,
  request: RPCRequest & { type: "write" },
): Generator<void, RPCResponse & { type: "write" }, ArrayBuffer> {
  const fid = storage.transactionSync(() => {
    const id = storage.sql.exec<{ id: number }>("SELECT id FROM ltx_file_id").one().id;
    storage.sql.exec("UPDATE ltx_file_id SET id = id + 1");
    return id;
  });
  span.setAttribute("file.id", fid);
  span.setAttribute("file.level", request.write_file.level);
  span.setAttribute("file.min_txid", request.write_file.min_txid);
  span.setAttribute("file.max_txid", request.write_file.max_txid);
  ws.serializeAttachment({
    ...attachment,
    state: "on-request",
    writingFileID: fid,
  });
  try {
    let idx = 0,
      size = 0;
    while (true) {
      const chunk = yield;
      if (chunk.byteLength > LTX_CHUNK_SIZE) {
        throw new Error(`Chunk size exceeds limit of ${LTX_CHUNK_SIZE} bytes`);
      } else if (chunk.byteLength === 0) {
        break;
      }
      storage.sql.exec(
        "INSERT INTO ltx_chunks (file_id, chunk_index, begin_offset, end_offset, body) VALUES (?, ?, ?, ?, ?)",
        fid,
        idx,
        size,
        size + chunk.byteLength,
        chunk,
      );
      idx++;
      size += chunk.byteLength;

      if (chunk.byteLength < LTX_CHUNK_SIZE) {
        break;
      }
    }
    storage.sql.exec(
      "INSERT INTO ltx_files (id, level, min_tx_id, max_tx_id, size, created_at) VALUES (?, ?, ?, ?, ?, ?)",
      fid,
      request.write_file.level,
      request.write_file.min_txid,
      request.write_file.max_txid,
      size,
      request.write_file.created_at,
    );

    ws.serializeAttachment({
      ...attachment,
      state: "waiting",
    });
    span.setAttribute("file.size", size);
    return {
      type: "write",
      file: {
        level: request.write_file.level,
        min_txid: request.write_file.min_txid,
        max_txid: request.write_file.max_txid,
        size,
        created_at: request.write_file.created_at,
      },
    };
  } catch (err) {
    storage.sql.exec("DELETE FROM ltx_chunks WHERE file_id = ?", fid);
    throw err;
  } finally {
    span.end();
  }
}

function migrate(storage: DurableObjectStorage): void {
  storage.transactionSync(() => {
    storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS ltx_migrations (
        version INTEGER PRIMARY KEY,
        applied_at INTEGER NOT NULL
      )
    `);
    const { version: maxVersion } = storage.sql
      .exec<{ version: number }>("SELECT MAX(version) AS version FROM ltx_migrations")
      .toArray()
      .at(0) ?? { version: -1 };
    for (const migration of MIGRATIONS) {
      if (migration.version <= maxVersion) continue;
      migration.apply(storage.sql);
      storage.sql.exec(
        "INSERT INTO ltx_migrations (version, applied_at) VALUES (?, ?)",
        migration.version,
        Date.now(),
      );
    }
  });
}

const MIGRATIONS: ReadonlyArray<{ version: number; apply: (sql: SqlStorage) => void }> = [
  {
    version: 1,
    apply: (sql) => {
      sql.exec("CREATE TABLE ltx_entries (id INTEGER PRIMARY KEY)");
      sql.exec("CREATE TABLE ltx_chunks (id INTEGER PRIMARY KEY)");
    },
  },
  {
    version: 2,
    apply: (sql) => {
      sql.exec("DROP TABLE ltx_entries");
      sql.exec("DROP TABLE ltx_chunks");
      sql.exec("CREATE TABLE ltx_file_id (id INTEGER PRIMARY KEY)");
      sql.exec("INSERT INTO ltx_file_id (id) VALUES (1)");
      sql.exec(`
        CREATE TABLE ltx_files (
          id INTEGER PRIMARY KEY,
          level INTEGER NOT NULL,
          min_tx_id TEXT NOT NULL,
          max_tx_id TEXT NOT NULL,
          size INTEGER NOT NULL DEFAULT 0,
          created_at INTEGER NOT NULL,
          UNIQUE (level, min_tx_id, max_tx_id)
        )
      `);
      sql.exec(`
        CREATE TABLE ltx_chunks (
          file_id INTEGER NOT NULL,
          chunk_index INTEGER NOT NULL,
          begin_offset INTEGER NOT NULL,
          end_offset INTEGER NOT NULL,
          body BLOB NOT NULL,
          PRIMARY KEY (file_id, chunk_index)
        )
      `);
    },
  },
];
