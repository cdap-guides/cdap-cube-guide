package co.cask.cdap.benchmarks.metrics.emitter;

import co.cask.cdap.api.metrics.Metrics;

/**
 *
 */
public interface Emitter {
  void emit(Metrics metrics, long currentTime, String emitterName, EmitterConfig config);
}
