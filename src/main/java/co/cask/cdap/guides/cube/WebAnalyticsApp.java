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

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.cube.AbstractCubeHttpHandler;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * An application that demonstrates real-time processing of Apache log entries, and storage using {@link Cube} dataset.
 */
public class WebAnalyticsApp extends AbstractApplication {
  static final String APP_NAME = "WebAnalyticsApp";
  static final String STREAM_NAME = "weblogs";
  static final String CUBE_NAME = "weblogsCube";
  static final String CUBE_NAME_2 = "testCube";
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

    DatasetProperties props2 = DatasetProperties.builder()
      .add("dataset.cube.resolutions", "1,60,3600")
      .add("dataset.cube.aggregation.agg1.dimensions", "dummy,another")
      .build();
    createDataset(CUBE_NAME_2, Cube.class, props2);

    addFlow(new CubeWriterFlow());
    addWorker(new CubeWriter());
    addService(SERVICE_NAME, new CubeHandler());
  }
}
