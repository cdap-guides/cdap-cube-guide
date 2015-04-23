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

package co.cask.cdap.guides.cube;

import co.cask.cdap.api.dataset.lib.cube.CubeExploreQuery;
import co.cask.cdap.api.dataset.lib.cube.CubeQuery;
import co.cask.cdap.api.dataset.lib.cube.MeasureType;
import co.cask.cdap.api.dataset.lib.cube.TagValue;
import co.cask.cdap.api.dataset.lib.cube.TimeSeries;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests covering the {@link co.cask.cdap.guides.cube.WebAnalyticsApp} application.
 */
public class WebAnalyticsAppTest extends TestBase {
  private static final Gson GSON = new Gson();

  @Test
  public void testApp() throws Exception {

    // Deploy the application
    ApplicationManager appManager = deployApplication(WebAnalyticsApp.class);

    // Start the flow
    FlowManager flowManager = appManager.startFlow(CubeWriterFlow.FLOW_NAME);
    try {
      // Load some data for querying
      StreamWriter streamWriter = appManager.getStreamWriter(WebAnalyticsApp.STREAM_NAME);

      long tsInSec =
        new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z").parse("08/Feb/2015:04:36:47 +0000").getTime() / 1000;
      streamWriter.send("69.181.160.120 - - [08/Feb/2015:04:36:47 +0000] " +
                          "\"GET /rest/api/latest/server?_=1423341312520 HTTP/1.1\" 200 45 " +
                          "\"http://builds.cask.co/browse/COOP-DBT-284/log\" " +
                          "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) " +
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 Safari/537.36\"");
      streamWriter.send("69.181.160.120 - - [08/Feb/2015:04:36:47 +0000] " +
                          "\"GET /rest/api/latest/server?_=1423341312520 HTTP/1.1\" 500 45 " +
                          "\"http://builds.cask.co/browse/COOP-DBT-284/log\" " +
                          "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) " +
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 Safari/537.36\"");

      // Wait until all stream events have been processed by the TrafficEventStore Flowlet
      RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics(WebAnalyticsApp.APP_NAME, CubeWriterFlow.FLOW_NAME, "writer");
      metrics.waitForProcessed(2, 7, TimeUnit.SECONDS);

      ServiceManager serviceManager = appManager.startService(WebAnalyticsApp.SERVICE_NAME);
      try {
        serviceManager.waitForStatus(true);
        URL url = serviceManager.getServiceURL();

        // search for tags
        Collection<TagValue> tags =
          searchTag(url, new CubeExploreQuery(tsInSec - 60, tsInSec + 60, 1, 100, new ArrayList<TagValue>()));
        Assert.assertEquals(3, tags.size());
        Iterator<TagValue> iterator = tags.iterator();
        TagValue tv = iterator.next();
        Assert.assertEquals("ip", tv.getTagName());
        Assert.assertEquals("69.181.160.120", tv.getValue());
        tv = iterator.next();
        Assert.assertEquals("response_status", tv.getTagName());
        Assert.assertEquals("200", tv.getValue());
        tv = iterator.next();
        Assert.assertEquals("response_status", tv.getTagName());
        Assert.assertEquals("500", tv.getValue());

        // search for measures
        Collection<String> measures =
          searchMeasure(url, new CubeExploreQuery(tsInSec - 60, tsInSec + 60, 1, 100, new ArrayList<TagValue>()));
        Assert.assertEquals(1, measures .size());
        String measure = measures.iterator().next();
        Assert.assertEquals("count", measure);

        // query for data
        Collection<TimeSeries> data =
          query(url, new CubeQuery("agg1", tsInSec - 60, tsInSec + 60, 1, 100,
                                   "count", MeasureType.COUNTER,
                                   new HashMap<String, String>(), new ArrayList<String>()));
        Assert.assertEquals(1, data.size());
        TimeSeries series = data.iterator().next();
        List<TimeValue> timeValues = series.getTimeValues();
        Assert.assertEquals(1, timeValues.size());
        TimeValue timeValue = timeValues.get(0);
        Assert.assertEquals(tsInSec, timeValue.getTimestamp());
        Assert.assertEquals(2, timeValue.getValue());

      } finally {
        serviceManager.stop();
        serviceManager.waitForStatus(false);
      }
    } finally {
      flowManager.stop();
    }
  }

  public static void main(String[] args) throws Exception {
    System.out.println(new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z").parse("08/Feb/2015:04:36:40 +0000").getTime());
    System.out.println(new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z").parse("08/Feb/2015:12:23:18 +0000").getTime());
  }
  private Collection<TagValue> searchTag(URL serviceUrl, CubeExploreQuery query) throws IOException {
    URL url = new URL(serviceUrl, "searchTag");
    HttpRequest request = HttpRequest.post(url).withBody(GSON.toJson(query)).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), new TypeToken<Collection<TagValue>>() {}.getType());
  }

  private Collection<String> searchMeasure(URL serviceUrl, CubeExploreQuery query) throws IOException {
    URL url = new URL(serviceUrl, "searchMeasure");
    HttpRequest request = HttpRequest.post(url).withBody(GSON.toJson(query)).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), new TypeToken<Collection<String>>() {}.getType());
  }

  private Collection<TimeSeries> query(URL serviceUrl, CubeQuery query) throws IOException {
    URL url = new URL(serviceUrl, "query");
    HttpRequest request = HttpRequest.post(url).withBody(GSON.toJson(query)).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), new TypeToken<Collection<TimeSeries>>() {}.getType());
  }
}
