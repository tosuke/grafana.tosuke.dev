import { env } from "cloudflare:workers";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { sizeDelimitedDecodeStream } from "@bufbuild/protobuf/wire";
import type { Timestamp } from "@bufbuild/protobuf/wkt";
import {
  MetricFamilySchema as PromMetricFamilySchema,
  MetricType as PromMetricType,
  type Exemplar as PromExemplar,
  type LabelPair as PromLabelPair,
  type MetricFamily as PromMetricFamily,
} from "@buf/prometheus_prometheus.bufbuild_es/io/prometheus/client/metrics_pb";
import {
  AggregationTemporality,
  type Exemplar,
  ExemplarSchema,
  type Metric,
  MetricSchema,
} from "@buf/opentelemetry_opentelemetry.bufbuild_es/opentelemetry/proto/metrics/v1/metrics_pb";
import {
  ExportMetricsServiceRequestSchema,
  ExportMetricsServiceResponseSchema,
  type ExportMetricsServiceRequest,
  type ExportMetricsServiceResponse,
} from "@buf/opentelemetry_opentelemetry.bufbuild_es/opentelemetry/proto/collector/metrics/v1/metrics_service_pb";
import {
  ATTR_DEPLOYMENT_ENVIRONMENT_NAME,
  ATTR_SERVICE_NAME,
} from "@opentelemetry/semantic-conventions";
import {
  type AnyValue,
  type KeyValue,
  KeyValueSchema,
} from "@buf/opentelemetry_opentelemetry.bufbuild_es/opentelemetry/proto/common/v1/common_pb";

export async function* scrapeMetrics(
  url: string | URL,
  options: {
    fetcher?: (req: Request) => Promise<Response>;
    scrapeTimestampMs?: bigint;
    signal?: AbortSignal;
  } = {},
) {
  const { fetcher = fetch, scrapeTimestampMs = BigInt(Date.now()), signal = null } = options;

  const resp = await fetcher(
    new Request(url, {
      signal,
      headers: {
        Accept:
          "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited",
      },
    }),
  );
  if (!resp.ok || !resp.body) {
    throw new Error(`Failed to fetch metrics: ${resp.status} ${await resp.text()}`);
  }
  for await (const metricFamily of sizeDelimitedDecodeStream(PromMetricFamilySchema, resp.body)) {
    yield* convertPromMetricToOtelMetric(metricFamily, scrapeTimestampMs);
  }
}

export function environmentResourceAttributes(): KeyValue[] {
  return attributes({
    [ATTR_SERVICE_NAME]: "grafana",
    "cloud.provider": "cloudflare",
    [ATTR_DEPLOYMENT_ENVIRONMENT_NAME]: env.ENV,
    "deployment.id": env.CF_VERSION_METADATA.id,
  });
}

export function attributes(attrs: Record<string, string | number | boolean>): KeyValue[] {
  return Object.entries(attrs).map(([key, value]) => {
    let anyValue: AnyValue["value"];
    switch (typeof value) {
      case "string":
        anyValue = { case: "stringValue", value };
        break;
      case "number":
        anyValue = { case: "doubleValue", value };
        break;
      case "boolean":
        anyValue = { case: "boolValue", value };
        break;
      default:
        throw new Error(`Unsupported attribute value type: ${typeof value}`);
    }
    return create(KeyValueSchema, {
      key,
      value: {
        value: anyValue,
      },
    });
  });
}

export function serviceNameAttribute(serviceName: string): KeyValue {
  return create(KeyValueSchema, {
    key: ATTR_SERVICE_NAME,
    value: {
      value: {
        case: "stringValue",
        value: serviceName,
      },
    },
  });
}

export async function exportMetricsAsOTLPHTTP(
  url: string,
  request: ExportMetricsServiceRequest,
  options?: {
    signal?: AbortSignal;
    fetcher?: (req: Request) => Promise<Response>;
    headers?: HeadersInit;
    compression?: "gzip";
  },
): Promise<ExportMetricsServiceResponse> {
  const { signal = null, fetcher = fetch } = options ?? {};

  const headers = new Headers(options?.headers);
  headers.set("Content-Type", "application/x-protobuf");
  if (options?.compression) {
    headers.set("Content-Encoding", options.compression);
  }

  let body = new Response(toBinary(ExportMetricsServiceRequestSchema, request)).body!;
  if (options?.compression) {
    body = body.pipeThrough(new CompressionStream(options.compression));
  }

  const resp = await fetcher(
    new Request(url, {
      method: "POST",
      headers,
      body,
      signal,
    }),
  );
  if (!resp.ok || !resp.body) {
    throw new Error(`Failed to export metrics: ${resp.status} ${await resp.text()}`);
  }

  return fromBinary(ExportMetricsServiceResponseSchema, new Uint8Array(await resp.arrayBuffer()));
}

export function convertPromMetricToOtelMetric(
  family: PromMetricFamily,
  defaultTimestampMs = BigInt(Date.now()),
): Metric[] {
  const metrics: Metric[] = [];
  const unit = unitWordToUCUM(family.unit);

  for (const promMetric of family.metric) {
    const attributes = promMetric.label.map(promLabelPairToOtelAttribute);
    const timeUnixNano = (promMetric.timestampMs || defaultTimestampMs) * 1_000_000n;

    switch (family.type) {
      case PromMetricType.COUNTER:
        const counter = promMetric.counter;
        if (!counter) continue;
        metrics.push(
          create(MetricSchema, {
            name: family.name,
            description: family.help,
            unit,
            data: {
              case: "sum",
              value: {
                dataPoints: [
                  {
                    attributes,
                    startTimeUnixNano: counter.startTimestamp
                      ? timestampToUnixNano(counter.startTimestamp)
                      : 0n,
                    timeUnixNano,
                    value: {
                      case: "asDouble",
                      value: counter.value,
                    },
                    exemplars: counter.exemplar
                      ? [promExemplarToOtelExemplar(counter.exemplar, timeUnixNano)]
                      : [],
                  },
                ],
                aggregationTemporality: AggregationTemporality.CUMULATIVE,
                isMonotonic: true,
              },
            },
          }),
        );
        break;
      case PromMetricType.GAUGE:
      case PromMetricType.UNTYPED:
        let value: number | undefined;
        if (family.type === PromMetricType.GAUGE) {
          value = promMetric.gauge?.value;
        } else {
          value = promMetric.untyped?.value;
        }
        if (value == null) continue;
        metrics.push(
          create(MetricSchema, {
            name: family.name,
            description: family.help,
            unit,
            data: {
              case: "gauge",
              value: {
                dataPoints: [
                  {
                    attributes,
                    timeUnixNano,
                    value: {
                      case: "asDouble",
                      value: value,
                    },
                  },
                ],
              },
            },
          }),
        );
        break;
      case PromMetricType.SUMMARY:
        const summary = promMetric.summary;
        if (!summary) continue;
        metrics.push(
          create(MetricSchema, {
            name: family.name,
            description: family.help,
            unit,
            data: {
              case: "summary",
              value: {
                dataPoints: [
                  {
                    attributes,
                    timeUnixNano,
                    startTimeUnixNano: summary.startTimestamp
                      ? timestampToUnixNano(summary.startTimestamp)
                      : 0n,
                    count: summary.sampleCount,
                    sum: summary.sampleSum,
                    quantileValues: summary.quantile.map((qv) => ({
                      quantile: qv.quantile,
                      value: qv.value,
                    })),
                  },
                ],
              },
            },
          }),
        );
        break;
      case PromMetricType.HISTOGRAM:
      case PromMetricType.GAUGE_HISTOGRAM:
        const histogram = promMetric.histogram;
        if (!histogram) continue;

        const aggregationTemporality =
          family.type === PromMetricType.HISTOGRAM
            ? AggregationTemporality.CUMULATIVE
            : AggregationTemporality.DELTA;

        const startTimeUnixNano = histogram.startTimestamp
          ? timestampToUnixNano(histogram.startTimestamp)
          : 0n;

        if (histogram.bucket.length > 0 /* explicit/classic histogram */) {
          //Prom: for i in 0..upperBound.length-1=cumulativeCount.length-1
          //  (upperBound[i-1] ?? -Inf, upperBound[i]]: cumulativeCount[i] - (cumulativeCount[i-1] ?? 0)
          //Otel: for i in 0..explicitBounds.length=bucketCounts.length-1
          //  (explicitBounds[i-1] ?? -Inf, explicitBounds[i] ?? +Inf]: bucketCounts[i]
          const buckets = histogram.bucket;
          const bounds = buckets.map((b) => b.upperBound).slice(0, -1);
          const cumulativeCounts = buckets.map((b) =>
            b.cumulativeCountFloat > 0
              ? BigInt(Math.floor(b.cumulativeCountFloat))
              : b.cumulativeCount,
          );
          const bucketCounts = cumulativeCounts.map((c, i) => c - (cumulativeCounts[i - 1] ?? 0n));
          const lastBound = buckets.at(-1)!.upperBound;
          if (Number.isFinite(lastBound)) {
            // add [lastBound, +Inf) bucket
            bounds.push(lastBound);
            bucketCounts.push(0n);
          }
          metrics.push(
            create(MetricSchema, {
              name: family.name,
              description: family.help,
              unit,
              data: {
                case: "histogram",
                value: {
                  aggregationTemporality,
                  dataPoints: [
                    {
                      attributes,
                      timeUnixNano,
                      startTimeUnixNano,
                      count:
                        histogram.sampleCountFloat > 0
                          ? BigInt(Math.floor(histogram.sampleCountFloat))
                          : histogram.sampleCount,
                      sum: histogram.sampleSum,
                      bucketCounts,
                      explicitBounds: bounds,
                      exemplars: histogram.bucket.flatMap((b) =>
                        b.exemplar ? [promExemplarToOtelExemplar(b.exemplar, timeUnixNano)] : [],
                      ),
                    },
                  ],
                },
              },
            }),
          );
        } else /* exponential histogram */ {
          const convertAbsoluteBuckets = (
            spans: typeof histogram.positiveSpan,
            counts: number[],
          ): bigint[] => {
            let countIdx = 0;
            const bucketCounts: bigint[] = [];
            for (const [spanIdx, span] of spans.entries()) {
              if (spanIdx > 0) {
                for (let i = 0; i < span.offset; i++) {
                  bucketCounts.push(0n);
                }
              }
              for (let i = 0; i < span.length; i++) {
                const count = counts[countIdx++] ?? 0;
                bucketCounts.push(BigInt(Math.floor(count)));
              }
            }
            return bucketCounts;
          };
          const convertDeltaBuckets = (
            spans: typeof histogram.positiveSpan,
            deltas: bigint[],
          ): bigint[] => {
            let deltaIdx = 0;
            let sum = 0n;
            const bucketCounts: bigint[] = [];
            for (const [spanIdx, span] of spans.entries()) {
              if (spanIdx > 0) {
                for (let i = 0; i < span.offset; i++) {
                  bucketCounts.push(0n);
                }
              }
              for (let i = 0; i < span.length; i++) {
                sum += deltas[deltaIdx++] ?? 0n;
                bucketCounts.push(sum);
              }
            }
            return bucketCounts;
          };
          metrics.push(
            create(MetricSchema, {
              name: family.name,
              description: family.help,
              unit,
              data: {
                case: "exponentialHistogram",
                value: {
                  aggregationTemporality,
                  dataPoints: [
                    {
                      attributes,
                      startTimeUnixNano,
                      timeUnixNano,
                      count:
                        histogram.sampleCountFloat > 0
                          ? BigInt(Math.floor(histogram.sampleCountFloat))
                          : histogram.sampleCount,
                      sum: histogram.sampleSum,
                      scale: histogram.schema, // 2^(2^-n)
                      zeroCount:
                        histogram.zeroCountFloat > 0
                          ? BigInt(Math.floor(histogram.zeroCountFloat))
                          : histogram.zeroCount,
                      zeroThreshold: histogram.zeroThreshold,
                      positive:
                        histogram.positiveSpan.length > 0
                          ? {
                              // Prom: (base^(i-1), base^i] -> Otel: (base^i, base^(i+1)]
                              offset: histogram.positiveSpan[0]!.offset - 1,
                              bucketCounts:
                                histogram.positiveCount.length > 0
                                  ? convertAbsoluteBuckets(
                                      histogram.positiveSpan,
                                      histogram.positiveCount,
                                    )
                                  : convertDeltaBuckets(
                                      histogram.positiveSpan,
                                      histogram.positiveDelta,
                                    ),
                            }
                          : undefined,
                      negative:
                        histogram.negativeSpan.length > 0
                          ? {
                              // Prom: [-base^i, -base^(i-1)) -> Otel: [-base^(i+1), -base^i)
                              offset: histogram.negativeSpan[0]!.offset - 1,
                              bucketCounts:
                                histogram.negativeCount.length > 0
                                  ? convertAbsoluteBuckets(
                                      histogram.negativeSpan,
                                      histogram.negativeCount,
                                    )
                                  : convertDeltaBuckets(
                                      histogram.negativeSpan,
                                      histogram.negativeDelta,
                                    ),
                            }
                          : undefined,
                      exemplars: histogram.exemplars.map((exemplar) =>
                        promExemplarToOtelExemplar(exemplar, timeUnixNano),
                      ),
                    },
                  ],
                },
              },
            }),
          );
        }
        break;
    }
  }

  return metrics;
}

function promLabelPairToOtelAttribute(pair: PromLabelPair): KeyValue {
  return create(KeyValueSchema, {
    key: pair.name,
    value: {
      value: {
        case: "stringValue",
        value: pair.value,
      },
    },
  });
}

function promExemplarToOtelExemplar(
  promExemplar: PromExemplar,
  defaultTimestampUnixNano = 0n,
): Exemplar {
  const filteredAttributes: KeyValue[] = [];
  let traceId = new Uint8Array(),
    spanId = new Uint8Array();
  for (const pair of promExemplar.label) {
    if (pair.name === "trace_id" && /^[0-9a-fA-F]{32}$/.test(pair.value)) {
      traceId = new Uint8Array(16);
      for (let i = 0; i < 32; i += 2) {
        traceId[i / 2] = parseInt(pair.value.slice(i, i + 2), 16);
      }
    } else if (pair.name === "span_id" && /^[0-9a-fA-F]{16}$/.test(pair.value)) {
      spanId = new Uint8Array(8);
      for (let i = 0; i < 16; i += 2) {
        spanId[i / 2] = parseInt(pair.value.slice(i, i + 2), 16);
      }
    } else {
      filteredAttributes.push(promLabelPairToOtelAttribute(pair));
    }
  }

  return create(ExemplarSchema, {
    filteredAttributes,
    timeUnixNano: promExemplar.timestamp
      ? timestampToUnixNano(promExemplar.timestamp)
      : defaultTimestampUnixNano,
    value: {
      case: "asDouble",
      value: promExemplar.value,
    },
    spanId,
    traceId,
  });
}

function timestampToUnixNano(timestamp: Timestamp): bigint {
  return timestamp.seconds * 1_000_000_000n + BigInt(timestamp.nanos);
}

const wordToUCUM: Record<string, string | undefined> = {
  // Time
  days: "d",
  hours: "h",
  minutes: "min",
  seconds: "s",
  milliseconds: "ms",
  microseconds: "us",
  nanoseconds: "ns",

  // Bytes
  bytes: "By",
  kibibytes: "KiBy",
  mebibytes: "MiBy",
  gibibytes: "GiBy",
  tibibytes: "TiBy",
  kilobytes: "KBy",
  megabytes: "MBy",
  gigabytes: "GBy",
  terabytes: "TBy",

  // SI
  meters: "m",
  volts: "V",
  amperes: "A",
  joules: "J",
  watts: "W",
  grams: "g",

  // Misc
  celsius: "Cel",
  hertz: "Hz",
  ratio: "1",
  percent: "%",
};

const perWordToUCUM: Record<string, string | undefined> = {
  second: "s",
  minute: "m",
  hour: "h",
  day: "d",
  week: "w",
  month: "mo",
  year: "y",
};

function unitWordToUCUM(unit: string): string {
  const [word, ...perWords] = unit.split("_per_");
  if (!word) return unit;
  const ucumWord = wordToUCUM[word] ?? word;
  const ucumPerWords = perWords.map((perWord) => perWordToUCUM[perWord] ?? perWord);
  return [ucumWord, ...ucumPerWords].join("/");
}
