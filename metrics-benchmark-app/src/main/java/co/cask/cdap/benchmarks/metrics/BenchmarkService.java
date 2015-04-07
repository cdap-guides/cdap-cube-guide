package co.cask.cdap.benchmarks.metrics;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.benchmarks.metrics.emitter.EmitterConfig;
import co.cask.cdap.benchmarks.metrics.emitter.EmitterConfigManager;
import co.cask.cdap.client.MetricsClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.proto.MetricQueryResult;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
public class BenchmarkService extends AbstractService {

  public static final String NAME = "BenchmarkService";

  @Override
  protected void configure() {
    setName(NAME);
    setDescription("Service that sets and gets rates, and gives reports.");
    addHandler(new EmitterConfigHandler());
    addHandler(new ReportHandler());
  }

  /**
   * Provides ways to set and get {@link co.cask.cdap.benchmarks.metrics.emitter.EmitterConfig}s.
   */
  public static final class EmitterConfigHandler extends AbstractHttpServiceHandler {

    private static final Gson GSON = new Gson();

    @UseDataSet("configs")
    private ObjectStore<EmitterConfig> configs;
    private EmitterConfigManager configManager;

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
      super.initialize(context);
      this.configManager = new EmitterConfigManager() {
        @Override
        protected EmitterConfig fetchConfig(String key) {
          return configs.read(key);
        }
      };
    }

    @GET
    @Path("config")
    public void getConfig(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendJson(ImmutableMap.of("delay", configManager.getConfig()));
    }

    @PUT
    @Path("config")
    public void setConfig(HttpServiceRequest request, HttpServiceResponder responder) {
      EmitterConfig input = GSON.fromJson(Bytes.toString(request.getContent()), EmitterConfig.class);
      configs.write("default", input);
      responder.sendStatus(200);
    }
  }

  /**
   * Provides ways to get a benchmark report.
   */
  public static final class ReportHandler extends AbstractHttpServiceHandler {

    private String hostname;

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
      super.initialize(context);
      this.hostname = context.getRuntimeArguments().get("hostname");
    }

    @GET
    @Path("report/start/{start}/end/{end}")
    public void getReport(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("start") String start, @PathParam("end") String end) {

      ClientConfig clientConfig = getClientConfig();
      MetricsClient metricsClient = new MetricsClient(clientConfig);

      Map<String, MetricQueryResult.TimeValue[]> result = Maps.newHashMap();
      String prefix = "system.";
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
          return (int) (o1.getTime() - o2.getTime());
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
