import { fromBinary } from "@bufbuild/protobuf";
import {
  ExportMetricsServiceRequestSchema,
  MetricsService,
} from "@buf/opentelemetry_opentelemetry.bufbuild_es/opentelemetry/proto/collector/metrics/v1/metrics_service_pb";
import { encodeEnvelope } from "@connectrpc/connect/protocol";
import { trailerFlag, trailerSerialize } from "@connectrpc/connect/protocol-grpc-web";
import { describe, expect, it, vi } from "vitest";
import { compressionGzip, createGrpcWebTransport } from "./grpc";

function grpcWebResponse(message: Uint8Array, trailer = new Headers({ "grpc-status": "0" })) {
  const messageEnvelope = encodeEnvelope(0, message);
  const trailerEnvelope = encodeEnvelope(trailerFlag, trailerSerialize(trailer));
  const body = new Uint8Array(messageEnvelope.byteLength + trailerEnvelope.byteLength);
  body.set(messageEnvelope);
  body.set(trailerEnvelope, messageEnvelope.byteLength);
  return new Response(body, {
    headers: { "Content-Type": "application/grpc-web+proto" },
  });
}

describe("createGrpcWebTransport", () => {
  it("makes a binary unary gRPC-Web request", async () => {
    let receivedRequest: Request | undefined;
    const fetcher = vi.fn<typeof fetch>(async (input, init) => {
      receivedRequest = new Request(input, init);
      return grpcWebResponse(new Uint8Array());
    });
    const transport = createGrpcWebTransport({
      baseUrl: "https://collector.example",
      fetcher,
    });

    const response = await transport.unary(
      MetricsService.method.export,
      undefined,
      undefined,
      new Headers({ "x-request-id": "test-request" }),
      { resourceMetrics: [] },
    );

    expect(response.message).toEqual(
      expect.objectContaining({
        $typeName: "opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse",
      }),
    );
    expect(fetcher).toHaveBeenCalledOnce();
    expect(receivedRequest).toBeDefined();
    if (!receivedRequest) return;

    expect(receivedRequest.method).toBe("POST");
    expect(receivedRequest.url).toBe(
      "https://collector.example/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
    );
    expect(receivedRequest.headers.get("content-type")).toBe("application/grpc-web+proto");
    expect(receivedRequest.headers.get("x-grpc-web")).toBe("1");
    expect(receivedRequest.headers.get("x-request-id")).toBe("test-request");

    const body = new Uint8Array(await receivedRequest.arrayBuffer());
    expect(body[0]).toBe(0);
    expect(body.subarray(5)).toHaveLength(body.byteLength - 5);
    expect(fromBinary(ExportMetricsServiceRequestSchema, body.subarray(5))).toEqual(
      expect.objectContaining({
        $typeName: "opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest",
        resourceMetrics: [],
      }),
    );
  });

  it("supports JSON request and response messages", async () => {
    let receivedRequest: Request | undefined;
    const fetcher = vi.fn<typeof fetch>(async (input, init) => {
      receivedRequest = new Request(input, init);
      return new Response(
        (() => {
          const message = new TextEncoder().encode("{}");
          const trailer = trailerSerialize(new Headers({ "grpc-status": "0" }));
          const messageEnvelope = encodeEnvelope(0, message);
          const trailerEnvelope = encodeEnvelope(trailerFlag, trailer);
          const body = new Uint8Array(messageEnvelope.byteLength + trailerEnvelope.byteLength);
          body.set(messageEnvelope);
          body.set(trailerEnvelope, messageEnvelope.byteLength);
          return body;
        })(),
        { headers: { "Content-Type": "application/grpc-web+json" } },
      );
    });
    const transport = createGrpcWebTransport({
      baseUrl: "https://collector.example/",
      fetcher,
      useBinaryFormat: false,
    });

    await expect(
      transport.unary(MetricsService.method.export, undefined, undefined, new Headers(), {
        resourceMetrics: [],
      }),
    ).resolves.toMatchObject({
      message: expect.objectContaining({
        $typeName: "opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse",
      }),
    });
    expect(receivedRequest).toBeDefined();
    if (!receivedRequest) return;
    expect(receivedRequest.headers.get("content-type")).toBe("application/grpc-web+json");
    const body = new Uint8Array(await receivedRequest.arrayBuffer());
    expect(new TextDecoder().decode(body.subarray(5))).toBe("{}");
  });
});

describe("compressionGzip", () => {
  it("compresses and decompresses data", async () => {
    const input = new TextEncoder().encode(
      "a payload that benefits from gzip compression ".repeat(100),
    );

    const compressed = await compressionGzip.compress(input);

    expect(compressed.byteLength).toBeLessThan(input.byteLength);
    await expect(compressionGzip.decompress(compressed, input.byteLength)).resolves.toEqual(input);
  });

  it("rejects decompressed data over the configured limit", async () => {
    const compressed = await compressionGzip.compress(new TextEncoder().encode("too large"));

    await expect(compressionGzip.decompress(compressed, 3)).rejects.toThrow(
      "Decompressed data exceeds readMaxBytes",
    );
  });
});
