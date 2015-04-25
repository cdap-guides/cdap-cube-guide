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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.cube.Cube;

/**
 * An application that demonstrates real-time processing of Apache log entries, and storage using {@link Cube} dataset.
 */
public class WebAnalyticsApp extends AbstractApplication {
  static final String APP_NAME = "WebAnalyticsApp";
  static final String STREAM_NAME = "weblogs";
  static final String CUBE_NAME = "weblogsCube";
  static final String SERVICE_NAME = "CubeService";

  @Override
  public void configure() {
    setName(APP_NAME);

    addStream(new Stream(STREAM_NAME));

    // configure the Cube dataset
    DatasetProperties props = DatasetProperties.builder()
      .add("dataset.cube.resolutions", "1,60,3600")
      .add("dataset.cube.aggregation.agg1.dimensions", "response_status")
      .add("dataset.cube.aggregation.agg2.dimensions", "ip,browser")
      .build();
    createDataset(CUBE_NAME, Cube.class, props);

    addFlow(new CubeWriterFlow());
    addService(SERVICE_NAME, new CubeHandler());
  }
}
