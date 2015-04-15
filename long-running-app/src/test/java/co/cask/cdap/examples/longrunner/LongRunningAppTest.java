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

package co.cask.cdap.examples.longrunner;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Long running app test.
 */
public class LongRunningAppTest extends TestBase {
  @Test
  public void testGetAtService() throws IOException, TimeoutException, InterruptedException {

    // record time

    // send 100 events to the stream todo : might have to send at the start of the timestamp if sending more

    // sleep for 2 seconds and do a get at the service for the recorded timestamp

    // make sure we get 100 events

    // Deploy the Application
    ApplicationManager appManager = deployApplication(LongRunningApp.class);

    // Start the Flow
    appManager.startFlow("LongRunningFlow");

    // Send a few events to the stream
    StreamWriter writer = appManager.getStreamWriter("eventStream");

    long timestampInMs = System.currentTimeMillis();

    for (int i = 0; i < 100; i++) {
      writer.send(timestampInMs + ":" + "payload" + i);
    }

    // Wait for the events to be processed, or at most 5 seconds
    RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("LongRunningApp", "LongRunningFlow", "flowlet4");
    metrics.waitForProcessed(100, 5, TimeUnit.SECONDS);

    ServiceManager longRunningAppServiceManager = appManager
      .startService(LongRunningService.SERVICE_NAME, ImmutableMap.<String, String>of());

    longRunningAppServiceManager.waitForStatus(true);

    URL serviceURL = longRunningAppServiceManager.getServiceURL(15, TimeUnit.SECONDS);
    Assert.assertNotNull(serviceURL);

    // Call the ping endpoint
    URL url = new URL(serviceURL, "processed/" + TimeUnit.MILLISECONDS.toSeconds(timestampInMs));
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    long value = Long.valueOf(response.getResponseBodyAsString());
    Assert.assertEquals(100L, value);
  }

  @Test
  public void testLatencyMetrics() throws Exception {
    final Map<String, Long> latencyBucketing = Maps.newHashMap();

    Metrics metrics = new Metrics() {
      @Override
      public void count(String metric, int i) {
        if (latencyBucketing.containsKey(metric)) {
          latencyBucketing.put(metric, latencyBucketing.get(metric) + i);
        } else {
          latencyBucketing.put(metric, (long) i);
        }
      }

      @Override
      public void gauge(String metric, long l) {
        latencyBucketing.put(metric, l);
      }
    };

    String metricName = "flow.test.latency";

    // emit latency in ms
    for (long i = 1; i < 11; i++ ) {
      LatencyMetric.emitMetrics(metrics, metricName, i);
    }
    for (long i = 20; i <= 100; i += 10 ) {
      LatencyMetric.emitMetrics(metrics, metricName, i);
    }

    LatencyMetric.emitMetrics(metrics, metricName, 400L);
    LatencyMetric.emitMetrics(metrics, metricName, 4000L);
    LatencyMetric.emitMetrics(metrics, metricName, 10000L);
    LatencyMetric.emitMetrics(metrics, metricName, 20000L);

    Assert.assertEquals(10L, (long) latencyBucketing.get("flow.test.latency.0-10ms"));
    Assert.assertEquals(9, (long) latencyBucketing.get("flow.test.latency.11-100ms"));
    Assert.assertEquals(1, (long) latencyBucketing.get("flow.test.latency.101-500ms"));
  }

  @Test
  public void testLastProcessedTimeStamp() throws IOException, TimeoutException, InterruptedException {


    // send 100 events to the stream todo : might have to send at the start of the timestamp if sending more
    // sleep for a second and repeat the above step for 5 times
    // record time

    // add  two 2 seconds and do a scan for that time at the service handler and check
    // for the recorded timestamp to match


    // Deploy the Application
    ApplicationManager appManager = deployApplication(LongRunningApp.class);

    // Start the Flow
    appManager.startFlow("LongRunningFlow");

    // Send a few events to the stream
    StreamWriter writer = appManager.getStreamWriter("eventStream");


    long timestampInMs = 0;
    for (int count = 0; count < 5; count++) {
      TimeUnit.SECONDS.sleep(1);
      timestampInMs = System.currentTimeMillis();
      for (int i = 0; i < 100; i++) {
        writer.send(timestampInMs + ":" + "payload" + i);
      }
    }
    long expectedTimeStamp = TimeUnit.MILLISECONDS.toSeconds(timestampInMs);

    // Wait for the events to be processed, or at most 5 seconds
    RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("LongRunningApp", "LongRunningFlow", "flowlet4");
    metrics.waitForProcessed(500, 5, TimeUnit.SECONDS);

    ServiceManager longRunningAppServiceManager = appManager
      .startService(LongRunningService.SERVICE_NAME, ImmutableMap.<String, String>of());

    longRunningAppServiceManager.waitForStatus(true);

    URL serviceURL = longRunningAppServiceManager.getServiceURL(15, TimeUnit.SECONDS);
    Assert.assertNotNull(serviceURL);

    // Call the ping endpoint
    URL url = new URL(serviceURL, "scanLast/" + (expectedTimeStamp + 5) + "/10?expected=100");
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    long value = Long.valueOf(response.getResponseBodyAsString());
    Assert.assertEquals(expectedTimeStamp, value);
  }

  @Test
  public void testLastProcessedTimeStampFromCurrentTime() throws IOException, TimeoutException, InterruptedException {


    // send 100 events to the stream todo : might have to send at the start of the timestamp if sending more
    // sleep for a second and repeat the above step for 5 times
    // record time

    // sleep for two seconds and do a scan for that time at the service handler and check
    // for the recorded timestamp to match


    // Deploy the Application
    ApplicationManager appManager = deployApplication(LongRunningApp.class);

    // Start the Flow
    appManager.startFlow("LongRunningFlow");

    // Send a few events to the stream
    StreamWriter writer = appManager.getStreamWriter("eventStream");


    long timestampInMs = 0;
    for (int count = 0; count < 5; count++) {
      TimeUnit.SECONDS.sleep(1);
      timestampInMs = System.currentTimeMillis();
      for (int i = 0; i < 100; i++) {
        writer.send(timestampInMs + ":" + "payload" + i);
      }
    }
    long expectedTimeStamp = TimeUnit.MILLISECONDS.toSeconds(timestampInMs);

    // Wait for the events to be processed, or at most 5 seconds
    RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("LongRunningApp", "LongRunningFlow", "flowlet4");
    metrics.waitForProcessed(500, 5, TimeUnit.SECONDS);

    ServiceManager longRunningAppServiceManager = appManager
      .startService(LongRunningService.SERVICE_NAME, ImmutableMap.<String, String>of());

    longRunningAppServiceManager.waitForStatus(true);

    URL serviceURL = longRunningAppServiceManager.getServiceURL(15, TimeUnit.SECONDS);
    Assert.assertNotNull(serviceURL);

    TimeUnit.SECONDS.sleep(3);
    // Call the ping endpoint
    URL url = new URL(serviceURL, "scanLast/10?expected=100");
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    long value = Long.valueOf(response.getResponseBodyAsString());
    Assert.assertEquals(expectedTimeStamp, value);
  }


}
