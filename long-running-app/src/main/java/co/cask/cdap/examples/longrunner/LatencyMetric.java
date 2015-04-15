package co.cask.cdap.examples.longrunner;

import co.cask.cdap.api.metrics.Metrics;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Emits latency metrics as counters under Predefined bucket ranges of latencies
 */
public class LatencyMetric {
  private static final long latency_buckets[] = {10000, 5000, 4000, 3000, 2000,1000, 500, 100, 10, 0};
  private static final Map<Long, String> bucketToName = ImmutableMap.<Long, String>builder().
    put(0L, "0-10ms").put(10L, "11-100ms").put(100L, "101-500ms").put(500L, "501-1000ms").
    put(1000L, "1001-2000ms").put(2000L, "2001-3000ms").put(3000L, "3001-4000ms").put(4000L, "4001-5000ms").
    put(5000L, "5001-10000ms").put(10000L, ">10000ms").build();

  public static void emitMetrics(Metrics metrics, String metricName, long latency) {
    // NOTE : okay to do linear search now, but can do binary search if buckets increase a lot.
    for (int i = 0; i < latency_buckets.length; i++) {
      if (latency > latency_buckets[i]) {
        metrics.count(metricName + "." + bucketToName.get(latency_buckets[i]), 1);
        break;
      }
    }
  }
}
