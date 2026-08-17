import { create } from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import {
  CounterSchema,
  ExemplarSchema as PromExemplarSchema,
  GaugeSchema,
  HistogramSchema as PromHistogramSchema,
  LabelPairSchema,
  MetricFamilySchema,
  MetricSchema as PromMetricSchema,
  MetricType,
  QuantileSchema,
  SummarySchema,
  UntypedSchema,
} from "@buf/prometheus_prometheus.bufbuild_es/io/prometheus/client/metrics_pb";
import {
  AggregationTemporality,
  type Metric,
} from "@buf/opentelemetry_opentelemetry.bufbuild_es/opentelemetry/proto/metrics/v1/metrics_pb";
import { describe, expect, it } from "vitest";
import { convertPromMetricToOtelMetric } from "./metric";

function label(name: string, value: string) {
  return create(LabelPairSchema, { name, value });
}

function family(
  type: MetricType,
  metric: ReturnType<typeof create<typeof PromMetricSchema>>,
  unit = "",
) {
  return create(MetricFamilySchema, {
    name: "test_metric",
    help: "A test metric",
    type,
    unit,
    metric: [metric],
  });
}

function metricData(metric: Metric) {
  expect(metric.name).toBe("test_metric");
  expect(metric.description).toBe("A test metric");
  expect(metric.unit).toBe("");
  return metric.data;
}

describe("convertPromMetricToOtelMetric", () => {
  it("converts counters, including labels, timestamps, and exemplars", () => {
    const metrics = convertPromMetricToOtelMetric(
      family(
        MetricType.COUNTER,
        create(PromMetricSchema, {
          label: [label("foo", "bar")],
          timestampMs: 1_234n,
          counter: create(CounterSchema, {
            value: 1234,
            startTimestamp: timestampFromMs(10_001),
            exemplar: create(PromExemplarSchema, {
              label: [
                label("trace_id", "0123456789abcdef0123456789abcdef"),
                label("span_id", "0123456789abcdef"),
                label("example", "value"),
              ],
              value: 12.34,
              timestamp: timestampFromMs(30_002),
            }),
          }),
        }),
      ),
    );

    expect(metrics).toHaveLength(1);
    const data = metricData(metrics[0]!);
    expect(data.case).toBe("sum");
    if (data.case !== "sum") return;

    expect(data.value.aggregationTemporality).toBe(AggregationTemporality.CUMULATIVE);
    expect(data.value.isMonotonic).toBe(true);
    expect(data.value.dataPoints).toHaveLength(1);
    const point = data.value.dataPoints[0]!;
    expect(point.attributes).toMatchObject([
      { key: "foo", value: { value: { case: "stringValue", value: "bar" } } },
    ]);
    expect(point.startTimeUnixNano).toBe(10_001_000_000n);
    expect(point.timeUnixNano).toBe(1_234_000_000n);
    expect(point.value).toEqual({ case: "asDouble", value: 1234 });
    expect(point.exemplars).toHaveLength(1);
    expect(point.exemplars[0]!.filteredAttributes).toMatchObject([
      { key: "example", value: { value: { case: "stringValue", value: "value" } } },
    ]);
    expect(Array.from(point.exemplars[0]!.traceId)).toEqual([
      0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd,
      0xef,
    ]);
    expect(Array.from(point.exemplars[0]!.spanId)).toEqual([
      0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
    ]);
    expect(point.exemplars[0]!.timeUnixNano).toBe(30_002_000_000n);
    expect(point.exemplars[0]!.value).toEqual({ case: "asDouble", value: 12.34 });
  });

  it.each([
    [MetricType.GAUGE, GaugeSchema, "gauge"],
    [MetricType.UNTYPED, UntypedSchema, "untyped"],
  ] as const)("converts %s to an OTLP gauge", (type, schema, field) => {
    const metrics = convertPromMetricToOtelMetric(
      family(
        type,
        create(PromMetricSchema, {
          timestampMs: 42n,
          [field]: create(schema, { value: 400.8 }),
        }),
      ),
    );

    expect(metrics).toHaveLength(1);
    const data = metricData(metrics[0]!);
    expect(data.case).toBe("gauge");
    if (data.case !== "gauge") return;
    expect(data.value.dataPoints).toEqual([
      expect.objectContaining({
        timeUnixNano: 42_000_000n,
        value: { case: "asDouble", value: 400.8 },
      }),
    ]);
  });

  it("uses the scrape timestamp when a Prometheus sample has no timestamp", () => {
    const metrics = convertPromMetricToOtelMetric(
      family(
        MetricType.GAUGE,
        create(PromMetricSchema, { gauge: create(GaugeSchema, { value: 1 }) }),
      ),
      1_700_000_000_000n,
    );

    const data = metricData(metrics[0]!);
    expect(data.case).toBe("gauge");
    if (data.case !== "gauge") return;
    expect(data.value.dataPoints[0]!.timeUnixNano).toBe(1_700_000_000_000_000_000n);
  });

  it("converts summaries and preserves the start timestamp and quantiles", () => {
    const metrics = convertPromMetricToOtelMetric(
      family(
        MetricType.SUMMARY,
        create(PromMetricSchema, {
          timestampMs: 2_000n,
          summary: create(SummarySchema, {
            startTimestamp: timestampFromMs(1_001),
            sampleCount: 1213n,
            sampleSum: 456,
            quantile: [
              create(QuantileSchema, { quantile: 0.5, value: 789 }),
              create(QuantileSchema, { quantile: 0.9, value: 1011 }),
            ],
          }),
        }),
      ),
    );

    expect(metrics).toHaveLength(1);
    const data = metricData(metrics[0]!);
    expect(data.case).toBe("summary");
    if (data.case !== "summary") return;
    expect(data.value.dataPoints).toEqual([
      expect.objectContaining({
        startTimeUnixNano: 1_001_000_000n,
        timeUnixNano: 2_000_000_000n,
        count: 1213n,
        sum: 456,
      }),
    ]);
    expect(
      data.value.dataPoints[0]!.quantileValues.map(({ quantile, value }) => ({ quantile, value })),
    ).toEqual([
      { quantile: 0.5, value: 789 },
      { quantile: 0.9, value: 1011 },
    ]);
  });

  it("converts a classic histogram to non-cumulative OTLP buckets", () => {
    const metrics = convertPromMetricToOtelMetric(
      family(
        MetricType.HISTOGRAM,
        create(PromMetricSchema, {
          timestampMs: 2_000n,
          histogram: create(PromHistogramSchema, {
            sampleCount: 1213n,
            sampleSum: 456,
            bucket: [
              { upperBound: 0.5, cumulativeCount: 789n },
              { upperBound: 10, cumulativeCount: 1011n },
              { upperBound: Number.POSITIVE_INFINITY, cumulativeCount: 1213n },
            ],
          }),
        }),
      ),
    );

    expect(metrics).toHaveLength(1);
    const data = metricData(metrics[0]!);
    expect(data.case).toBe("histogram");
    if (data.case !== "histogram") return;
    expect(data.value.aggregationTemporality).toBe(AggregationTemporality.CUMULATIVE);
    expect(data.value.dataPoints).toEqual([
      expect.objectContaining({
        timeUnixNano: 2_000_000_000n,
        startTimeUnixNano: 0n,
        count: 1213n,
        sum: 456,
        bucketCounts: [789n, 222n, 202n],
        explicitBounds: [0.5, 10],
        exemplars: [],
      }),
    ]);
  });

  it("converts gauge histograms with delta temporality", () => {
    const metrics = convertPromMetricToOtelMetric(
      family(
        MetricType.GAUGE_HISTOGRAM,
        create(PromMetricSchema, {
          timestampMs: 3_000n,
          histogram: create(PromHistogramSchema, {
            startTimestamp: timestampFromMs(5_001),
            sampleCount: 3n,
            sampleSum: 7,
            bucket: [{ upperBound: Number.POSITIVE_INFINITY, cumulativeCount: 3n }],
          }),
        }),
      ),
    );

    const data = metricData(metrics[0]!);
    expect(data.case).toBe("histogram");
    if (data.case !== "histogram") return;
    expect(data.value.aggregationTemporality).toBe(AggregationTemporality.DELTA);
    expect(data.value.dataPoints[0]).toMatchObject({
      startTimeUnixNano: 5_001_000_000n,
      bucketCounts: [3n],
      explicitBounds: [],
    });
  });

  it("converts native integer histograms and keeps exemplar values", () => {
    const metrics = convertPromMetricToOtelMetric(
      family(
        MetricType.HISTOGRAM,
        create(PromMetricSchema, {
          timestampMs: 4_000n,
          histogram: create(PromHistogramSchema, {
            schema: 3,
            sampleCount: 1213n,
            sampleSum: 456,
            zeroThreshold: 0.001,
            zeroCount: 2n,
            negativeSpan: [
              { offset: 0, length: 1 },
              { offset: 1, length: 1 },
            ],
            negativeDelta: [1n, 1n],
            positiveSpan: [
              { offset: -2, length: 1 },
              { offset: 1, length: 1 },
            ],
            positiveDelta: [1n, 0n],
            exemplars: [
              create(PromExemplarSchema, { value: 0.2, timestamp: timestampFromMs(123_000) }),
              create(PromExemplarSchema, { value: 1.3, timestamp: timestampFromMs(321_000) }),
            ],
          }),
        }),
      ),
    );

    expect(metrics).toHaveLength(1);
    const data = metricData(metrics[0]!);
    expect(data.case).toBe("exponentialHistogram");
    if (data.case !== "exponentialHistogram") return;
    const point = data.value.dataPoints[0]!;
    expect(point).toMatchObject({
      timeUnixNano: 4_000_000_000n,
      startTimeUnixNano: 0n,
      count: 1213n,
      sum: 456,
      scale: 3,
      zeroCount: 2n,
      zeroThreshold: 0.001,
    });
    expect(point.negative).toMatchObject({ offset: -1, bucketCounts: [1n, 0n, 2n] });
    expect(point.positive).toMatchObject({ offset: -3, bucketCounts: [1n, 0n, 1n] });
    expect(point.exemplars.map((exemplar) => exemplar.value)).toEqual([
      { case: "asDouble", value: 0.2 },
      { case: "asDouble", value: 1.3 },
    ]);
  });

  it("uses float counts for native float-counter histograms", () => {
    const metrics = convertPromMetricToOtelMetric(
      family(
        MetricType.HISTOGRAM,
        create(PromMetricSchema, {
          timestampMs: 4_000n,
          histogram: create(PromHistogramSchema, {
            schema: -1,
            sampleCountFloat: 1213.9,
            sampleSum: 456,
            zeroThreshold: 0.001,
            zeroCountFloat: 2.9,
            negativeSpan: [
              { offset: 0, length: 1 },
              { offset: 1, length: 1 },
            ],
            negativeCount: [1.5, 2.5],
            positiveSpan: [
              { offset: -2, length: 1 },
              { offset: 2, length: 1 },
            ],
            positiveCount: [1, 3],
          }),
        }),
      ),
    );

    const data = metricData(metrics[0]!);
    expect(data.case).toBe("exponentialHistogram");
    if (data.case !== "exponentialHistogram") return;
    expect(data.value.dataPoints[0]).toMatchObject({
      count: 1213n,
      zeroCount: 2n,
      negative: { offset: -1, bucketCounts: [1n, 0n, 2n] },
      positive: { offset: -3, bucketCounts: [1n, 0n, 0n, 3n] },
    });
  });

  it("converts units to UCUM notation", () => {
    const metrics = convertPromMetricToOtelMetric(
      family(
        MetricType.GAUGE,
        create(PromMetricSchema, {
          timestampMs: 1_000n,
          gauge: create(GaugeSchema, { value: 1 }),
        }),
        "bytes_per_second",
      ),
    );

    expect(metrics[0]!.unit).toBe("By/s");
  });

  it("skips metrics whose payload does not match the family type", () => {
    const metrics = convertPromMetricToOtelMetric(
      family(
        MetricType.COUNTER,
        create(PromMetricSchema, { gauge: create(GaugeSchema, { value: 1 }) }),
      ),
    );

    expect(metrics).toEqual([]);
  });
});
