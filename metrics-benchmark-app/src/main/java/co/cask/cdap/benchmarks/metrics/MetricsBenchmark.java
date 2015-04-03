/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.benchmarks.metrics;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.client.MetricsClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.proto.MetricQueryResult;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
public class MetricsBenchmark extends AbstractApplication {

  private static final int DEFAULT_DELAY = 100;

  @Override
  public void configure() {
    setName("MetricsBenchmark");
    setDescription("A metrics benchmark application");
    createDataset("delays", KeyValueTable.class);
    addWorker(new MetricsEmitter());
    addService(new BenchmarkService());
  }

  /**
   * Emits metrics at a specified rate.
   */
  public static final class MetricsEmitter extends AbstractWorker {

    public static final String NAME = "MetricsEmitter";

    private Metrics metrics;

    private String instanceName;

    /**
     * How long to wait between emitting metrics, in milliseconds.
     */
    private int delay;

    private long delayRefreshRate = 1000;
    private long lastRefreshedDelay;

    @Override
    protected void configure() {
      setName(NAME);
      useDatasets("delays");
    }

    @Override
    public void initialize(WorkerContext context) throws Exception {
      super.initialize(context);
      instanceName = "emitter-" + context.getInstanceId();
    }

    @Override
    public void run() {
      while(true) {
        // get latest rate
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastRefreshedDelay >= delayRefreshRate) {
          delay = getLatestDelay();
          lastRefreshedDelay = currentTime;
        }

        metrics.count(instanceName + "-" + currentTime, 1);
        try {
          Thread.sleep(delay);
        } catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
      }
    }

    // TODO: repeated code
    private int getLatestDelay() {
      final AtomicInteger delay = new AtomicInteger();
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext datasetContext) throws Exception {
          KeyValueTable delays = datasetContext.getDataset("delays");

          byte[] instanceDelay = delays.read(instanceName);
          if (instanceDelay != null) {
            delay.set(Bytes.toInt(instanceDelay));
            return;
          }

          byte[] defaultDelay = delays.read("default");
          delay.set(defaultDelay == null ? DEFAULT_DELAY : Bytes.toInt(defaultDelay));
        }
      });
      return delay.get();
    }
  }

  /**
   * A {@link Service} for metrics benchmark.
   */
  public static final class BenchmarkService extends AbstractService {

    public static final String NAME = "BenchmarkService";

    @Override
    protected void configure() {
      setName(NAME);
      setDescription("Service that sets and gets rates, and gives reports.");
      addHandler(new RateHandler());
      addHandler(new ReportHandler());
    }
  }

  /**
   * Provides ways to set and get rates.
   */
  public static final class RateHandler extends AbstractHttpServiceHandler {

    private static final Gson GSON = new Gson();

    @UseDataSet("delays")
    private KeyValueTable delays;

    @GET
    @Path("delays/emitter/{emitter-id}")
    public void getEmitter(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("emitter-id") String emitterId) {
      responder.sendJson(ImmutableMap.of("delay", getLatestDelay("emitter-" + emitterId)));
    }

    @PUT
    @Path("delays/emitter/{emitter-id}")
    public void setEmitter(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("emitter-id") String emitterId) {
      JsonObject input = GSON.fromJson(Bytes.toString(request.getContent()), JsonObject.class);
      delays.write("emitter-" + emitterId, Bytes.toBytes(input.get("delay").getAsInt()));
      responder.sendStatus(200);
    }

    @GET
    @Path("delays/default")
    public void getDefault(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendJson(ImmutableMap.of("delay", getLatestDelay("default")));
    }

    @PUT
    @Path("delays/default")
    public void setDefault(HttpServiceRequest request, HttpServiceResponder responder) {
      JsonObject input = GSON.fromJson(Bytes.toString(request.getContent()), JsonObject.class);
      delays.write("default", Bytes.toBytes(input.get("delay").getAsInt()));
      responder.sendStatus(200);
    }

    // TODO: repeated code
    private int getLatestDelay(String instanceName) {
      Preconditions.checkState(delays != null);
      byte[] instanceDelay = delays.read(instanceName);
      if (instanceDelay != null) {
        return Bytes.toInt(instanceDelay);
      }

      byte[] defaultDelay = delays.read("default");
      return defaultDelay == null ? DEFAULT_DELAY : Bytes.toInt(defaultDelay);
    }
  }

  /**
   * Provides ways to get a benchmark report.
   */
  public static final class ReportHandler extends AbstractHttpServiceHandler {

    private String hostname;

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
      this.hostname = context.getRuntimeArguments().get("hostname");
    }

    @GET
    @Path("report/start/{start}/end/{end}")
    public void getReport(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("start") String start, @PathParam("end") String end) {

      ClientConfig clientConfig = getClientConfig();
      MetricsClient metricsClient = new MetricsClient(clientConfig);

      Map<String, MetricQueryResult.TimeValue[]> result = Maps.newHashMap();
      String prefix = "system.metrics.global.";
      result.put("processed.rate", getMetrics(metricsClient, prefix + "processed.count", start, end));
      result.put("processed.delay.min", getMetrics(metricsClient, prefix + "processed.delay.min", start, end));
      result.put("processed.delay.max", getMetrics(metricsClient, prefix + "processed.delay.max", start, end));
      result.put("processed.delay.avg", getMetrics(metricsClient, prefix + "processed.delay.avg", start, end));

//      MetricQueryResult.TimeValue[] rateMetrics;
//      if (countMetrics.length <= 1) {
//        rateMetrics = new MetricQueryResult.TimeValue[0];
//      } else {
//        rateMetrics = new MetricQueryResult.TimeValue[countMetrics.length - 1];
//        for (int i = 1; i < countMetrics.length; i++) {
//          MetricQueryResult.TimeValue countMetric = countMetrics[i];
//          MetricQueryResult.TimeValue previousCountMetric = countMetrics[i - 1];
//
//          long deltaTime = countMetric.getTime() - previousCountMetric.getTime();
//          long rate = (long) ((countMetric.getValue() - previousCountMetric.getValue()) * 1.0 / deltaTime);
//          rateMetrics[i - 1] = new MetricQueryResult.TimeValue(countMetric.getTime(), rate);
//        }
//      }
//      result.put("processed.rate", rateMetrics);

      responder.sendJson(200, result);
    }

    private MetricQueryResult.TimeValue[] sortMetrics(MetricQueryResult.TimeValue[] metrics) {
      Arrays.sort(metrics, new Comparator<MetricQueryResult.TimeValue>() {
        @Override
        public int compare(MetricQueryResult.TimeValue o1, MetricQueryResult.TimeValue o2) {
          return (int) (o1.getTime() -  o2.getTime());
        }
      });
      return metrics;
    }

    private MetricQueryResult.TimeValue[] getMetrics(MetricsClient metricsClient, String metricName,
                                                     String start, String end) {
      try {
        MetricQueryResult queryResult = metricsClient.query(metricName, null, start, end, null);
        if (queryResult == null ||
          queryResult.getSeries() == null ||
          queryResult.getSeries().length == 0 ||
          queryResult.getSeries()[0].getData() == null) {
          return new MetricQueryResult.TimeValue[0];
        }

        return sortMetrics(queryResult.getSeries()[0].getData());
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    private ClientConfig getClientConfig() {
      ConnectionConfig connectionConfig = ConnectionConfig.builder()
        .setHostname(hostname)
        .build();
      return ClientConfig.builder()
        .setConnectionConfig(connectionConfig)
        .build();
    }

  }
}
