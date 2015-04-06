package co.cask.cdap.benchmarks.metrics.emitter;

import co.cask.cdap.api.metrics.Metrics;

/**
 *
 */
public class UniqueEmitter implements Emitter {
  @Override
  public void emit(Metrics metrics, long currentTime, String emitterName, EmitterConfig config) {
    metrics.count(emitterName + "-" + currentTime, 1);
  }
}
