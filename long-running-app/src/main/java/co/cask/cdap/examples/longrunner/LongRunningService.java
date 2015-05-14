package co.cask.cdap.examples.longrunner;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Long running service
 */
public class LongRunningService extends AbstractService {
  public static final String SERVICE_NAME = "LongRunningService";
  private static final byte[] column = Bytes.toBytes("t");

  @Override
  protected void configure() {
    setName(SERVICE_NAME);
    setDescription("A service to retrieve details about the long running app's status");
    addHandler(new LongRunningServiceHandler());
  }


  /**
   * Service for retrieving a customer’s purchase history.
   */
  public static final class LongRunningServiceHandler extends AbstractHttpServiceHandler {

    @UseDataSet("inputProcessed")
    private Table timeStampData;

    private Metrics metrics;

    /**
     * Scans last x minutes and determine how much we are behind in sec
     *
     * @param minutes in past to scan
     */
    @Path("scanLast/{minutes}")
    @GET
    public void scanLastXMinutes(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("minutes") int minutes, @QueryParam("expected") long expected) {
      long currentTimeInMs = System.currentTimeMillis();
      long currentTime = TimeUnit.MILLISECONDS.toSeconds(currentTimeInMs);
      long pastCutoffTime = currentTime - (minutes * 60);

      byte[] startRow = Bytes.toBytes(Long.MAX_VALUE - currentTime);
      byte[] endRow = Bytes.toBytes(Long.MAX_VALUE - pastCutoffTime);

      String metricsPrefix = "scan.last." + minutes + "min.";
      scanData(startRow, endRow, expected, minutes, currentTimeInMs, metricsPrefix, responder);
    }


    /**
     * Scans last x minutes from a given timestamp and determine how much we are behind in sec
     *
     * @param minutes in past to scan
     */
    @Path("scanLast/{timestamp}/{minutes}")
    @GET
    public void scanRangeMinutes(HttpServiceRequest request, HttpServiceResponder responder,
                                 @PathParam("timestamp") int timestamp,
                                 @PathParam("minutes") int minutes, @QueryParam("expected") long expected) {
      long currentTimeInMs = System.currentTimeMillis();
      long pastCutoffTime = timestamp - (minutes * 60);

      byte[] startRow = Bytes.toBytes(Long.MAX_VALUE - timestamp);
      byte[] endRow = Bytes.toBytes(Long.MAX_VALUE - pastCutoffTime);

      String metricsPrefix = "scan.range." + minutes + "min.";
      scanData(startRow, endRow, expected, minutes, currentTimeInMs, metricsPrefix, responder);
    }

    /**
     * gets entries processed at a timestamp
     */
    @Path("processed/{timestamp}")
    @GET
    public void getProcessedAt(HttpServiceRequest request, HttpServiceResponder responder,
                                 @PathParam("timestamp") int timestamp) {

      long startTimeInMs = System.currentTimeMillis();
      byte[] rowKey = Bytes.toBytes(Long.MAX_VALUE - timestamp);
      Row row = timeStampData.get(rowKey);
      long processed = 0;

      if (row != null && row.get(column) != null) {
        processed = Bytes.toLong(row.get(column));
      }

      if (processed == 0) {
        responder.sendError(500, "No records processed at " + timestamp);
        return;
      }

      long latency = System.currentTimeMillis() - startTimeInMs;
      String metricsPrefix = "get.at." + timestamp + ".";
      metrics.gauge( metricsPrefix + "latency", latency);
      metrics.gauge( metricsPrefix + "requests", processed);
      responder.sendJson(200, processed);
    }

    private void scanData(byte[] startRow, byte[] endRow, long expected, int minutes,
                          long startTime, String metricsPrefix, HttpServiceResponder responder) {
      Scanner scanner = timeStampData.scan(startRow, endRow);
      Row rowResult;
      int entriesProcessed = 0;
      long lastProcessedTimestamp = 0;
      while ((rowResult = scanner.next()) != null) {
        long value = rowResult.getLong(column);
        entriesProcessed += value;
        if (value == expected) {
          lastProcessedTimestamp = Bytes.toLong(rowResult.getRow());
          break;
        }
      }

      if (lastProcessedTimestamp == 0) {
        responder.sendError(500, "Expected throughput wasn't processed in last" + minutes + "minutes");
        return;
      }

      long latency = System.currentTimeMillis() - startTime;
      long delay = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - lastProcessedTimestamp;

      metrics.gauge(metricsPrefix + "latency", latency);
      metrics.gauge(metricsPrefix + "delay", delay);
      metrics.count(metricsPrefix + "requests", entriesProcessed);
      responder.sendJson(200, (Long.MAX_VALUE - lastProcessedTimestamp));
    }

  }

}
