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
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;

import java.util.concurrent.TimeUnit;

/**
 * Simple flow for parsing weblogs and writing them to a {@link co.cask.cdap.api.dataset.lib.cube.Cube}.
 */
public class CubeWriterFlow extends AbstractFlow {
  static final String FLOW_NAME = "CubeWriterFlow";

  @Override
  public void configure() {
    setName(FLOW_NAME);
    setDescription("Reads logs from a Stream and writes them to a Cube dataset");
    addFlowlet("writer", new CubeWriterFlowlet());
    connectStream(WebAnalyticsApp.STREAM_NAME, "writer");
  }
}
