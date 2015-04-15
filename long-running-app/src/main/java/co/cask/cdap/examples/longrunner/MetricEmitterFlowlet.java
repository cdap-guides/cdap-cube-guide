package co.cask.cdap.examples.longrunner;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;

import javax.annotation.Nullable;

/**
 * Receives stream event, emits metric and passes it to next flowlet
 */
public class MetricEmitterFlowlet extends AbstractFlowlet {
  private OutputEmitter<StreamEvent> out;
  Metrics metrics;

  @Nullable
  protected Long getTimestamp(StreamEvent event) {
    String body = Bytes.toString(event.getBody());
    String[] tokens =  body.split(":");
    if (tokens.length == 2) {
      return Long.parseLong(tokens[0]);
    }
    return null;
  }

  protected void emitMetric(long timestamp) {
    // ignore payload
    LatencyMetric.emitMetrics(metrics, "lat", System.currentTimeMillis() - timestamp);
  }

  @ProcessInput
  public void process(StreamEvent event) {
    Long timestamp = getTimestamp(event);
    if (timestamp == null) {
      // No need to even emit the event forward if it is not an a known format.
      return;
    }
    emitMetric(timestamp);
    out.emit(event);
  }
}
