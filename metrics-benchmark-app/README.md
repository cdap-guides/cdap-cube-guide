# Metrics Benchmark Application

This application helps benchmark metrics performance.

There are a few main components:
* `EmitterConfig`: governs how many metrics events to send per second
* `EmitterConfigManager`: handles refresh of the emitter config every 3 seconds
* `MetricsEmitterFlow`: emits a batch of metrics every second -- batch size is from the current emitter config
* `BenchmarkService`: exposes HTTP APIs to set and get the emitter config, and also to get the relevant metrics-collector metrics

## Running (e.g. 10 clients/flowlets, 100 metrics events per second)

Notes:
* the emitter config defaults to a batch size of 10

To start the benchmark from the CLI:

```
start service MetricsBenchmark.BenchmarkService

# once BenchmarkService is running..
call service MetricsBenchmark.BenchmarkService PUT /config body '{"batchSize":100}'
set flowlet instances MetricsBenchmark.MetricsEmitterFlow.emitter 10
start flow MetricsBenchmark.MetricsEmitterFlow
```

Then, to query the collector metrics:

```
curl -XPOST <base-uri>/v3/metrics/query?context=component.metrics~processor&metric=system.processed.count&start=now-30s&end=now | python -mjson.tool
curl -XPOST <base-uri>/v3/metrics/query?context=component.metrics~processor&metric=system.processed.delay.avg&start=now-30s&end=now | python -mjson.tool
```

