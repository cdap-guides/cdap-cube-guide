/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.api.dataset.lib.cube.MeasureType;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses web logs from {@link StreamEvent} and writes to a {@link Cube} dataset.
 */
public class CubeWriterFlowlet extends AbstractFlowlet {
  /*
     Example log line:

     69.181.160.120 - - [08/Feb/2015:04:36:40 +0000] "GET /ajax/foo?buildNumber=284&_=1423341312519 HTTP/1.1" 200 508
     "http://builds.cask.co/browse/COOP-DBT-284/log" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36
     (KHTML, like Gecko) Chrome/38.0.2125.122 Safari/537.36"
  */
  private static final Pattern CLF_PATTERN =
    Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] " +
                      "\"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"");

  // Example of formatted date: "08/Feb/2015:04:36:40 +0000"
  private static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");

  private Metrics metrics;

  @UseDataSet(WebAnalyticsApp.CUBE_NAME)
  private Cube cube;

  @ProcessInput
  public void process(StreamEvent event) throws Exception {

    String logEntryLine = Bytes.toString(event.getBody());

    Pattern p = CLF_PATTERN;
    Matcher matcher = p.matcher(logEntryLine);
    if (!matcher.matches()) {
      metrics.count("parse.errors", 1);
      return;
    }

    // creating CubeFact with timestamp of the log record
    long ts = DATE_FORMAT.parse(matcher.group(4)).getTime();
    CubeFact fact = new CubeFact(ts / 1000);

    // adding tags (dimensions)
    fact.addDimensionValue("ip", matcher.group(1));
    fact.addDimensionValue("request", matcher.group(5));
    fact.addDimensionValue("response_status", matcher.group(6));
    if (!matcher.group(8).equals("-")) {
      fact.addDimensionValue("referrer", matcher.group(8));
    }
    fact.addDimensionValue("browser", matcher.group(9));

    // adding measurements
    fact.addMeasurement("count", MeasureType.COUNTER, 1);
    Integer bytesSent = Integer.valueOf(matcher.group(7));
    fact.addMeasurement("bytes.sent", MeasureType.COUNTER, bytesSent);
    cube.add(fact);
  }
}
