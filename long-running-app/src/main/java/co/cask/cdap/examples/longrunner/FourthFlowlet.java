package co.cask.cdap.examples.longrunner;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Receives stream event, emits metric and writes the arrival latency from emit time to a dataset
 */
public class FourthFlowlet extends AbstractFlowlet {
  private static final byte[] column = Bytes.toBytes("t");

  @UseDataSet("inputProcessed")
  private Table timeStampData;
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

  @ProcessInput
  public void process(StreamEvent event) {
    Long timestamp = getTimestamp(event);
    if (timestamp != null) {
      LatencyMetric.emitMetrics(metrics, "lat", System.currentTimeMillis() - timestamp);

      // round timestamp to seconds and invert it
      byte[] rowKey = Bytes.toBytes(Long.MAX_VALUE - TimeUnit.MILLISECONDS.toSeconds(timestamp));
      // todo verify increment by 1
      timeStampData.increment(rowKey, column, 1);
    }
  }
}
