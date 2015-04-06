package co.cask.cdap.benchmarks.metrics.emitter;

/**
 *
 */
public class EmitterConfig {

  private final int batchSize;
  private final long delayMs;

  public EmitterConfig(int batchSize, long delayMs) {
    this.batchSize = batchSize;
    this.delayMs = delayMs;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public long getDelayMs() {
    return delayMs;
  }
}
