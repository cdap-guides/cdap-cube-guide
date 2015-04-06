package co.cask.cdap.benchmarks.metrics.emitter;

import co.cask.cdap.api.metrics.Metrics;

/**
 *
 */
public class CommonSetEmitter implements Emitter {
  @Override
  public void emit(Metrics metrics, long currentTime, String emitterName, EmitterConfig config) {
    for (int i = 0; i < config.getBatchSize(); i++) {
      metrics.gauge("metric-" + i, currentTime);
    }
  }
}
