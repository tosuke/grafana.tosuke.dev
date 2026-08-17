import type {
  BinaryReadOptions,
  BinaryWriteOptions,
  JsonReadOptions,
  JsonWriteOptions,
} from "@bufbuild/protobuf";
import type { Interceptor, Transport } from "@connectrpc/connect";
import { createFetchClient, type Compression } from "@connectrpc/connect/protocol";
import { createTransport } from "@connectrpc/connect/protocol-grpc-web";

export type TransportOptions = {
  fetcher?: typeof fetch;
  baseUrl: string;
  useBinaryFormat?: boolean;
  interceptors?: Interceptor[];
  jsonOptions?: Partial<JsonReadOptions & JsonWriteOptions>;
  binaryOptions?: Partial<BinaryReadOptions & BinaryWriteOptions>;
  acceptCompression?: Compression[];
  sendCompression?: Compression;
  compressMinBytes?: number;
  writeMaxBytes?: number;
  readMaxBytes?: number;
};

export function createGrpcWebTransport({
  fetcher = fetch,
  baseUrl,
  useBinaryFormat = true,
  interceptors = [],
  jsonOptions = {},
  binaryOptions = {},
  acceptCompression = [],
  sendCompression,
  compressMinBytes = 1024,
  writeMaxBytes = 1 << 20, // 1 MiB
  readMaxBytes = 1 << 20, // 1 MiB
}: TransportOptions): Transport {
  return createTransport({
    httpClient: createFetchClient(fetcher),
    baseUrl,
    useBinaryFormat,
    interceptors,
    jsonOptions,
    binaryOptions,
    acceptCompression,
    sendCompression: sendCompression ?? null,
    compressMinBytes,
    writeMaxBytes,
    readMaxBytes,
  });
}

function createCompression(name: "gzip" | "deflate"): Compression {
  return {
    name,
    async compress(bytes): Promise<Uint8Array<ArrayBuffer>> {
      const compressReadable = new Response(bytes).body!.pipeThrough(new CompressionStream(name));
      return new Uint8Array(await new Response(compressReadable).arrayBuffer());
    },
    async decompress(bytes, readMaxBytes): Promise<Uint8Array<ArrayBuffer>> {
      const decompressReadable = new Response(bytes).body!.pipeThrough(
        new DecompressionStream(name),
      );
      const chunks = [];
      let size = 0;
      for await (const chunk of decompressReadable) {
        size += chunk.byteLength;
        chunks.push(chunk);
        if (size > readMaxBytes) {
          throw new Error("Decompressed data exceeds readMaxBytes");
        }
      }
      const decompressed = new Uint8Array(size);
      let offset = 0;
      for (const chunk of chunks) {
        decompressed.set(chunk, offset);
        offset += chunk.byteLength;
      }
      return decompressed;
    },
  };
}

export const compressionGzip = createCompression("gzip");
