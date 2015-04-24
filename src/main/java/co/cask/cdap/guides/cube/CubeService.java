/*
 * Copyright 2015 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.cube.AbstractCubeHttpHandler;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.Service;


/**
 * A {@link Service} to retrieve statistics from a {@link WebAnalyticsApp#CUBE_NAME} dataset.
 */
public class CubeService extends AbstractService {

  public static final String SERVICE_NAME = "CubeService";
  public static final String SERVICE_DESCRIPTION = "A service to retrieve statistics and query a " +
                                                     WebAnalyticsApp.CUBE_NAME;

  @Override
  protected void configure() {
    setName(SERVICE_NAME);
    setDescription(SERVICE_DESCRIPTION);
    addHandler(new CubeHandler());
  }

  /**
   * {@link AbstractCubeHttpHandler} implementation that uses a {@link WebAnalyticsApp#CUBE_NAME} dataset.
   */
  public final class CubeHandler extends AbstractCubeHttpHandler {

    @UseDataSet(WebAnalyticsApp.CUBE_NAME)
    private Cube cube;

    @Override
    protected Cube getCube() {
      return cube;
    }
  }
}




