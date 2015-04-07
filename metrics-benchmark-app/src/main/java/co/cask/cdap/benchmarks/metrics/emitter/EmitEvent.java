package co.cask.cdap.benchmarks.metrics.emitter;

/**
 *
 */
public class EmitEvent {

  private final int batchSize;

  public EmitEvent(int batchSize) {
    this.batchSize = batchSize;
  }

  public int getBatchSize() {
    return batchSize;
  }
}
