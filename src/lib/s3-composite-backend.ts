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
import * as z from "zod/mini";

export type S3BackendRoute = {
  readonly prefix: string;
  readonly backend: S3Backend;
};

type RouteSource = {
  readonly id: string;
  readonly route: S3BackendRoute | undefined;
  readonly backend: S3Backend;
  readonly prefix: string;
};

type SourceState = {
  readonly source: RouteSource;
  cursor: string | undefined;
  startAfter: string | undefined;
  exhausted: boolean;
  head: Head | undefined;
};

type Head = {
  readonly object: S3ObjectMetadata;
  readonly nextCursor: string | undefined;
  readonly exhausted: boolean;
};

type LogicalEntry =
  | { readonly kind: "object"; readonly key: string; readonly object: S3ObjectMetadata }
  | { readonly kind: "prefix"; readonly key: string };

const cursorSourceSchema = z.strictObject({
  id: z.string(),
  cursor: z.optional(z.string()),
  startAfter: z.optional(z.string()),
  exhausted: z.boolean(),
});

type CursorSource = z.infer<typeof cursorSourceSchema>;

const compositeCursorSchema = z.strictObject({
  version: z.literal(1),
  prefix: z.string(),
  delimiter: z.nullable(z.string()),
  sources: z.array(cursorSourceSchema),
});

type CompositeCursor = z.infer<typeof compositeCursorSchema>;

const DEFAULT_SOURCE_ID = "\u0000default";
const textEncoder = new TextEncoder();

export class CompositeS3Backend implements S3Backend {
  readonly #routes: readonly S3BackendRoute[];
  readonly #defaultBackend: S3Backend | undefined;

  constructor(routes: readonly S3BackendRoute[], defaultBackend?: S3Backend) {
    const copied = [...routes].sort((left, right) => this.#compareUtf8(left.prefix, right.prefix));
    for (const route of copied) {
      if (route.prefix.length === 0) throw new Error("Backend route prefix must not be empty");
      if (route.prefix === DEFAULT_SOURCE_ID) {
        throw new Error(`Reserved backend route prefix: ${route.prefix}`);
      }
    }
    for (let index = 0; index < copied.length; index++) {
      for (let other = index + 1; other < copied.length; other++) {
        const leftRoute = copied[index];
        const rightRoute = copied[other];
        if (leftRoute === undefined || rightRoute === undefined) continue;
        const left = leftRoute.prefix;
        const right = rightRoute.prefix;
        if (left === right) throw new Error(`Duplicate backend route prefix: ${left}`);
        if (left.startsWith(right) || right.startsWith(left)) {
          throw new Error(`Overlapping backend route prefixes: ${left}, ${right}`);
        }
      }
    }
    this.#routes = copied;
    this.#defaultBackend = defaultBackend;
  }

  async getObject(key: string, options: S3GetOptions): Promise<S3GetResult> {
    const backend = this.#backendFor(key);
    return backend === undefined ? { kind: "not-found" } : backend.getObject(key, options);
  }

  async putObject(
    key: string,
    body: ReadableStream<Uint8Array> | null,
    options: S3PutOptions,
  ): Promise<S3ObjectMetadata> {
    return this.#requiredBackend(key).putObject(key, body, options);
  }

  async deleteObjects(keys: readonly string[]): Promise<void> {
    const partitions = new Map<S3Backend, string[]>();
    for (const key of keys) {
      const backend = this.#backendFor(key);
      if (backend === undefined) continue;
      const partition = partitions.get(backend);
      if (partition === undefined) partitions.set(backend, [key]);
      else partition.push(key);
    }
    await Promise.all(
      [...partitions].map(([backend, backendKeys]) => backend.deleteObjects(backendKeys)),
    );
  }

  async listObjects(options: S3ListOptions): Promise<S3ListResult> {
    if (options.limit <= 0) return { objects: [], delimitedPrefixes: [], truncated: false };

    const enclosingRoute = this.#routes.find((route) => options.prefix.startsWith(route.prefix));
    if (enclosingRoute !== undefined) {
      return enclosingRoute.backend.listObjects(options);
    }

    const sources = this.#sourcesFor(options.prefix);
    const states = this.#statesFor(sources, options);
    for (const state of states) await this.#ensureHead(state);

    const objects: S3ObjectMetadata[] = [];
    const delimitedPrefixes: string[] = [];
    while (objects.length + delimitedPrefixes.length < options.limit) {
      const next = this.#minimum(states, options.prefix, options.delimiter);
      if (next === undefined) {
        return { objects, delimitedPrefixes, truncated: false };
      }

      if (next.entry.kind === "object") {
        objects.push(next.entry.object);
        await this.#consume(next.state);
        await this.#ensureHead(next.state);
      } else {
        delimitedPrefixes.push(next.entry.key);
        for (const state of states) {
          while (true) {
            await this.#ensureHead(state);
            const head = state.head;
            if (
              head === undefined ||
              this.#logical(head.object, options.prefix, options.delimiter).key !== next.entry.key
            ) {
              break;
            }
            await this.#consume(state);
          }
        }
      }
    }

    const lookahead = states.map((state) => this.#copyState(state));
    for (const state of lookahead) await this.#ensureHead(state);
    const truncated = this.#minimum(lookahead, options.prefix, options.delimiter) !== undefined;
    if (!truncated) return { objects, delimitedPrefixes, truncated: false };

    return {
      objects,
      delimitedPrefixes,
      truncated: true,
      cursor: this.#encodeCursor(this.#cursorFor(states, options)),
    };
  }

  async createMultipartUpload(key: string, options: S3PutOptions): Promise<S3MultipartUpload> {
    return this.#requiredBackend(key).createMultipartUpload(key, options);
  }

  async uploadPart(
    key: string,
    uploadId: string,
    partNumber: number,
    body: ReadableStream<Uint8Array> | null,
  ): Promise<S3MultipartPart> {
    return this.#requiredBackend(key).uploadPart(key, uploadId, partNumber, body);
  }

  async completeMultipartUpload(
    key: string,
    uploadId: string,
    parts: readonly S3MultipartPart[],
  ): Promise<S3ObjectMetadata> {
    return this.#requiredBackend(key).completeMultipartUpload(key, uploadId, parts);
  }

  async abortMultipartUpload(key: string, uploadId: string): Promise<void> {
    return this.#requiredBackend(key).abortMultipartUpload(key, uploadId);
  }

  #backendFor(key: string): S3Backend | undefined {
    const route = this.#routes.find(({ prefix }) => key.startsWith(prefix));
    return route?.backend ?? this.#defaultBackend;
  }

  #requiredBackend(key: string): S3Backend {
    const backend = this.#backendFor(key);
    if (backend === undefined) throw new Error(`No backend for key: ${key}`);
    return backend;
  }

  #sourcesFor(prefix: string): readonly RouteSource[] {
    const sources: RouteSource[] = [];
    for (const route of this.#routes) {
      if (!route.prefix.startsWith(prefix)) continue;
      sources.push({
        id: route.prefix,
        route,
        backend: route.backend,
        prefix: route.prefix,
      });
    }
    if (this.#defaultBackend !== undefined) {
      sources.push({
        id: DEFAULT_SOURCE_ID,
        route: undefined,
        backend: this.#defaultBackend,
        prefix,
      });
    }
    return sources;
  }

  #statesFor(sources: readonly RouteSource[], options: S3ListOptions): SourceState[] {
    if (options.cursor === undefined) {
      return sources.map((source) => ({
        source,
        cursor: undefined,
        startAfter: options.startAfter,
        exhausted: false,
        head: undefined,
      }));
    }

    const cursor = this.#decodeCursor(options.cursor);
    if (
      cursor.prefix !== options.prefix ||
      cursor.delimiter !== (options.delimiter === undefined ? null : options.delimiter) ||
      cursor.sources.length !== sources.length ||
      cursor.sources.some((entry, index) => entry.id !== sources[index]?.id)
    ) {
      throw new Error("Invalid cursor");
    }
    return sources.map((source, index) => {
      const entry: CursorSource | undefined = cursor.sources[index];
      if (entry === undefined) throw new Error("Invalid cursor");
      if (entry.cursor !== undefined && entry.startAfter !== undefined) {
        throw new Error("Invalid cursor");
      }
      return {
        source,
        cursor: entry.cursor,
        startAfter: entry.startAfter,
        exhausted: entry.exhausted,
        head: undefined,
      };
    });
  }

  async #ensureHead(state: SourceState): Promise<void> {
    while (state.head === undefined && !state.exhausted) {
      const result = await state.source.backend.listObjects({
        prefix: state.source.prefix,
        limit: 1,
        ...(state.cursor !== undefined ? { cursor: state.cursor } : {}),
        ...(state.startAfter !== undefined ? { startAfter: state.startAfter } : {}),
      });
      if (result.delimitedPrefixes.length !== 0) {
        throw new Error("Backend returned delimited prefixes without a delimiter");
      }
      if (result.objects.length > 1) throw new Error("Backend returned too many objects");
      const object = result.objects[0];
      const nextCursor = result.truncated ? result.cursor : undefined;
      const exhausted = !result.truncated;
      if (object === undefined) {
        state.cursor = nextCursor;
        state.startAfter = undefined;
        state.exhausted = exhausted;
        continue;
      }
      if (!this.#includeObject(state.source, object)) {
        state.cursor = nextCursor;
        state.startAfter = undefined;
        state.exhausted = exhausted;
        continue;
      }
      state.head = { object, nextCursor, exhausted };
    }
  }

  #includeObject(source: RouteSource, object: S3ObjectMetadata): boolean {
    if (!object.key.startsWith(source.prefix)) return false;
    if (source.route !== undefined) return object.key.startsWith(source.route.prefix);
    return !this.#routes.some(({ prefix }) => object.key.startsWith(prefix));
  }

  async #consume(state: SourceState): Promise<void> {
    const head = state.head;
    if (head === undefined) throw new Error("Cannot consume an empty source");
    state.cursor = head.nextCursor;
    state.startAfter = undefined;
    state.exhausted = head.exhausted;
    state.head = undefined;
  }

  #copyState(state: SourceState): SourceState {
    return {
      source: state.source,
      cursor: state.cursor,
      startAfter: state.startAfter,
      exhausted: state.exhausted,
      head: state.head,
    };
  }

  #minimum(
    states: readonly SourceState[],
    prefix: string,
    delimiter: string | undefined,
  ): { state: SourceState; entry: LogicalEntry } | undefined {
    let minimum: { state: SourceState; entry: LogicalEntry } | undefined;
    for (const state of states) {
      if (state.head === undefined) continue;
      const entry = this.#logical(state.head.object, prefix, delimiter);
      if (minimum === undefined || this.#compareUtf8(entry.key, minimum.entry.key) < 0) {
        minimum = { state, entry };
      }
    }
    return minimum;
  }

  #logical(object: S3ObjectMetadata, prefix: string, delimiter: string | undefined): LogicalEntry {
    if (delimiter === undefined) return { kind: "object", key: object.key, object };
    const suffix = object.key.slice(prefix.length);
    const index = suffix.indexOf(delimiter);
    if (index < 0) return { kind: "object", key: object.key, object };
    return { kind: "prefix", key: prefix + suffix.slice(0, index + delimiter.length) };
  }

  #cursorFor(states: readonly SourceState[], options: S3ListOptions): CompositeCursor {
    return {
      version: 1,
      prefix: options.prefix,
      delimiter: options.delimiter === undefined ? null : options.delimiter,
      sources: states.map(({ source, cursor, startAfter, exhausted }) => ({
        id: source.id,
        ...(cursor !== undefined ? { cursor } : {}),
        ...(startAfter !== undefined ? { startAfter } : {}),
        exhausted,
      })),
    };
  }

  #compareUtf8(left: string, right: string): number {
    const leftBytes = textEncoder.encode(left);
    const rightBytes = textEncoder.encode(right);
    const length = Math.min(leftBytes.length, rightBytes.length);
    for (let index = 0; index < length; index++) {
      const leftByte = leftBytes[index];
      const rightByte = rightBytes[index];
      if (leftByte === undefined || rightByte === undefined) continue;
      if (leftByte < rightByte) return -1;
      if (leftByte > rightByte) return 1;
    }
    return leftBytes.length - rightBytes.length;
  }

  #encodeCursor(cursor: CompositeCursor): string {
    return textEncoder
      .encode(JSON.stringify(cursor))
      .toBase64({ alphabet: "base64url", omitPadding: true });
  }

  #decodeCursor(value: string): CompositeCursor {
    try {
      const bytes = Uint8Array.fromBase64(value, { alphabet: "base64url" });
      const text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(bytes);
      const parsed: unknown = JSON.parse(text);
      return compositeCursorSchema.parse(parsed);
    } catch {
      throw new Error("Invalid cursor");
    }
  }
}
