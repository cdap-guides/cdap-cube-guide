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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;

/**
 * LongRunningApp
 */
public class LongRunningApp extends AbstractApplication {

  public static final String APP_NAME = "LongRunningApp";

  @Override
  public void configure() {
    setName(APP_NAME);
    setDescription("Long running application.");

    // Ingest data into the Application via a Stream
    addStream(new Stream("eventStream"));

    // TTL = one week in ms
    long ttl = (60 * 60 * 24 * 7 * 1000);
    DatasetProperties properties = DatasetProperties.builder().add(Table.PROPERTY_TTL,
                                                                   String.valueOf(ttl)).build();
    // Store processed data in a Dataset
    createDataset("inputProcessed", Table.class, properties);

    // Process events in realtime using a Flow
    addFlow(new LongRunningFlow());

    // Retrieve the processed data using a Service
    addService(new LongRunningService());

  }
}
