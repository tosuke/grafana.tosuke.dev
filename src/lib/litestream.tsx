/** @jsxImportSource hono/jsx */
/** @jsxRuntime automatic */

import { RpcTarget } from "cloudflare:workers";
import { Hono } from "hono";
import { html } from "hono/html";
import type { Child } from "hono/jsx";
import * as z from "zod";

export function createLitestreamApp(getLitestream: () => Rpc.Result<Litestream>): Hono {
  const app = new Hono();

  app.get("/", async (c) => {
    const litestream = getLitestream();
    using info = await litestream.info();
    using list = await litestream.list();
    using status = await litestream.status();
    const refreshedAt = new Date().toISOString();

    return c.html(
      <Dashboard
        info={info}
        databases={list.databases}
        statuses={status}
        refreshedAt={refreshedAt}
      />,
    );
  });

  app.get("/db", async (c) => {
    const path = c.req.query("path");
    if (!path) return c.text("Missing path query parameter", 400);

    const litestream = getLitestream();

    using list = await litestream.list();
    using status = await litestream.status();
    using files = await litestream.ltx(path);
    return c.html(
      <DatabaseDetail
        path={path}
        database={list.databases.filter((d) => d.path === path).at(0)}
        status={status.filter((s) => s.database === path).at(0)}
        files={files}
      />,
    );
  });

  return app;
}

function formatDate(value: string): string {
  return new Intl.DateTimeFormat("en", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  }).format(new Date(value));
}

function formatUptime(seconds: number): string {
  const days = Math.floor(seconds / 86_400);
  const hours = Math.floor((seconds % 86_400) / 3_600);
  const minutes = Math.floor((seconds % 3_600) / 60);
  const remainingSeconds = seconds % 60;

  if (days > 0) return `${days}d ${hours}h ${minutes}m`;
  if (hours > 0) return `${hours}h ${minutes}m ${remainingSeconds}s`;
  if (minutes > 0) return `${minutes}m ${remainingSeconds}s`;
  return `${remainingSeconds}s`;
}

const InfoResultSchema = z.object({
  version: z.string(),
  pid: z.number().int().positive(),
  uptime_seconds: z.number().int().nonnegative(),
  started_at: z.iso.datetime(),
  database_count: z.number().int().nonnegative(),
});

const ListResultSchema = z.object({
  databases: z.array(
    z.object({
      path: z.string(),
      status: z.enum(["replicating", "open", "stopped"]),
      last_sync_at: z.iso.datetime(),
    }),
  ),
});

const StatusResultSchema = z.array(
  z.object({
    database: z.string(),
    status: z.enum(["ok", "not initialized", "no database", "error"]),
    local_txid: z.string(),
    wal_size: z.string(),
  }),
);

const LtxResultSchema = z.array(
  z.object({
    level: z.number().int().min(0).max(9),
    min_txid: z.string(),
    max_txid: z.string(),
    size: z.number().int().nonnegative(),
    timestamp: z.iso.datetime(),
  }),
);

type InfoResult = z.infer<typeof InfoResultSchema>;
type Database = z.infer<typeof ListResultSchema>["databases"][number];
type DatabaseStatus = z.infer<typeof StatusResultSchema>[number];
type LtxFile = z.infer<typeof LtxResultSchema>[number];

function PageLayout({ title, children }: { title: string; children: Child }) {
  return (
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        {html`
          <link
            rel="stylesheet"
            href="https://cdn.jsdelivr.net/npm/@exampledev/new.css@1.1.3/new.min.css"
          />
          <style>
            body {
              max-width: 1100px;
            }

            table {
              width: 100%;
            }

            td,
            th {
              vertical-align: top;
            }

            code {
              overflow-wrap: anywhere;
            }
          </style>
        `}
        <title>{title}</title>
      </head>
      <body>{children}</body>
    </html>
  );
}

function SiteHeader({
  title,
  description,
  children,
}: {
  title: string;
  description?: string;
  children?: Child;
}) {
  return (
    <header>
      <p>
        <small>Grafana · replication service</small>
      </p>
      {children}
      <h1>{title}</h1>
      {description && <p>{description}</p>}
    </header>
  );
}

function Dashboard({
  info,
  databases,
  statuses,
  refreshedAt,
}: {
  info: InfoResult;
  databases: Database[];
  statuses: DatabaseStatus[];
  refreshedAt: string;
}) {
  return (
    <PageLayout title="Litestream dashboard">
      <SiteHeader
        title="Litestream dashboard"
        description="Monitor the embedded Litestream process and its SQLite replicas."
      />
      <main>
        <section aria-labelledby="overview-heading">
          <h2 id="overview-heading">Overview</h2>
          <article>
            <dl>
              <div>
                <dt>Version</dt>
                <dd>
                  <code>{info.version}</code>
                </dd>
              </div>
              <div>
                <dt>Process ID</dt>
                <dd>{info.pid}</dd>
              </div>
              <div>
                <dt>Uptime</dt>
                <dd>{formatUptime(info.uptime_seconds)}</dd>
              </div>
              <div>
                <dt>Started</dt>
                <dd>
                  <time datetime={info.started_at}>{formatDate(info.started_at)}</time>
                </dd>
              </div>
              <div>
                <dt>Databases</dt>
                <dd>{info.database_count}</dd>
              </div>
            </dl>
          </article>
        </section>

        <section aria-labelledby="databases-heading">
          <h2 id="databases-heading">Databases</h2>
          <article>
            <DatabaseTable databases={databases} statuses={statuses} />
          </article>
        </section>
      </main>

      <footer>
        <p>
          <small>
            Last refreshed <time datetime={refreshedAt}>{formatDate(refreshedAt)}</time>
          </small>
        </p>
      </footer>
    </PageLayout>
  );
}

function DatabaseTable({
  databases,
  statuses,
}: {
  databases: Database[];
  statuses: DatabaseStatus[];
}) {
  const statusByDatabase = new Map(statuses.map((status) => [status.database, status]));
  return (
    <table>
      <thead>
        <tr>
          <th scope="col">Path</th>
          <th scope="col">Replication</th>
          <th scope="col">Local status</th>
          <th scope="col">Local txid</th>
          <th scope="col">WAL size</th>
          <th scope="col">Last sync</th>
        </tr>
      </thead>
      <tbody>
        {databases.length === 0 ? (
          <tr>
            <td colspan={6}>No databases configured.</td>
          </tr>
        ) : (
          databases.map((database) => (
            <tr>
              <th scope="row">
                <a href={`/_litestream/db?path=${encodeURIComponent(database.path)}`}>
                  <code>{database.path}</code>
                </a>
              </th>
              <td>
                <mark>{database.status}</mark>
              </td>
              <td>
                <mark>{statusByDatabase.get(database.path)?.status ?? "unknown"}</mark>
              </td>
              <td>
                <code>{statusByDatabase.get(database.path)?.local_txid ?? "-"}</code>
              </td>
              <td>{statusByDatabase.get(database.path)?.wal_size ?? "-"}</td>
              <td>
                <time datetime={database.last_sync_at}>{formatDate(database.last_sync_at)}</time>
              </td>
            </tr>
          ))
        )}
      </tbody>
    </table>
  );
}

function DatabaseDetail({
  path,
  database,
  status,
  files,
}: {
  path: string;
  database: Database | undefined;
  status: DatabaseStatus | undefined;
  files: LtxFile[];
}) {
  return (
    <PageLayout title={`${path} · Litestream dashboard`}>
      <SiteHeader title="Database details">
        <p>
          <a href="/_litestream">← Litestream dashboard</a>
        </p>
        <p>
          <code>{path}</code>
        </p>
      </SiteHeader>

      <main>
        <section aria-labelledby="status-heading">
          <h2 id="status-heading">Status</h2>
          <article>
            <DatabaseTable
              databases={database ? [database] : []}
              statuses={status ? [status] : []}
            />
          </article>
        </section>
        <section aria-labelledby="ltx-heading">
          <h2 id="ltx-heading">LTX files</h2>
          <article>
            <table>
              <thead>
                <tr>
                  <th scope="col">Level</th>
                  <th scope="col">Min txid</th>
                  <th scope="col">Max txid</th>
                  <th scope="col">Size</th>
                  <th scope="col">Created</th>
                </tr>
              </thead>
              <tbody>
                <LtxRows files={files} />
              </tbody>
            </table>
          </article>
        </section>
      </main>
    </PageLayout>
  );
}

function LtxRows({ files }: { files: LtxFile[] }) {
  if (files.length === 0) {
    return (
      <tr>
        <td colspan={5}>No LTX files found.</td>
      </tr>
    );
  }

  return files.map((file) => (
    <tr>
      <td>{file.level}</td>
      <td>
        <code>{file.min_txid}</code>
      </td>
      <td>
        <code>{file.max_txid}</code>
      </td>
      <td>{formatBytes(file.size)}</td>
      <td>
        <time datetime={file.timestamp}>{formatDate(file.timestamp)}</time>
      </td>
    </tr>
  ));
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 ** 2) return `${Math.round(bytes / 1024)} kB`;
  return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
}

export abstract class Litestream extends RpcTarget {
  static create(...args: ConstructorParameters<typeof LitestreamImpl>): Litestream {
    return new LitestreamImpl(...args);
  }
  abstract info(): Promise<z.infer<typeof InfoResultSchema>>;
  abstract list(): Promise<z.infer<typeof ListResultSchema>>;
  abstract status(): Promise<z.infer<typeof StatusResultSchema>>;
  abstract ltx(path: string): Promise<z.infer<typeof LtxResultSchema>>;
}

const decoder = new TextDecoder();

class LitestreamImpl extends Litestream {
  constructor(
    private ctx: DurableObjectState,
    private binPath: string,
    private socketPath: string = "/var/run/litestream.sock",
    private configPath: string = "/etc/litestream.yml",
  ) {
    super();
  }

  async info() {
    return InfoResultSchema.parse(
      await this.run(this.binPath, "info", "-json", "-socket", this.socketPath),
    );
  }

  async list() {
    return ListResultSchema.parse(
      await this.run(this.binPath, "list", "-json", "-socket", this.socketPath),
    );
  }

  async status() {
    return StatusResultSchema.parse(
      await this.run(this.binPath, "status", "-json", "-config", this.configPath),
    );
  }

  async ltx(path: string) {
    return LtxResultSchema.parse(
      await this.run(
        this.binPath,
        "ltx",
        "-level",
        "all",
        "-json",
        "-config",
        this.configPath,
        path,
      ),
    );
  }

  async run(...args: string[]) {
    const container = this.ctx.container;
    if (!container || !container.running) {
      throw new Error("Container is not running");
    }
    const process = await container.exec(args);
    const output = await process.output();
    if (output.exitCode !== 0) {
      throw new Error(decoder.decode(output.stderr));
    }
    return JSON.parse(decoder.decode(output.stdout));
  }
}
