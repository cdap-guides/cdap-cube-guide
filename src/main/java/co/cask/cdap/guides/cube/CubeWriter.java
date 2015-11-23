package co.cask.cdap.guides.cube;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.api.dataset.lib.cube.MeasureType;
import co.cask.cdap.api.worker.AbstractWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class CubeWriter extends AbstractWorker {
  private volatile boolean stopped;
  private  static final Logger LOG = LoggerFactory.getLogger(CubeWriter.class);
  @Override
  public void run() {
    writeToTable();
    while (!stopped) {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {

      }
    }
  }

  @Override
  public void destroy() {
    // do nothing
  }

  @Override
  public void stop() {
    stopped = true;
  }


  private void writeToTable() {

    long currentTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    long startTimeInSeconds = currentTimeInSeconds - (TimeUnit.DAYS.toSeconds(730));
    LOG.info("Start time is {}", startTimeInSeconds);
    int count = 0;
    List<CubeFact> cubeFacts = new ArrayList<>();
    for (long ts = startTimeInSeconds; ts < currentTimeInSeconds; ts += 2880) {
      count++;
      CubeFact fact = new CubeFact(ts);
      fact.addDimensionValue("dummy", "data" + count);
      fact.addDimensionValue("another", "a-data" + count);
      fact.addMeasurement("count", MeasureType.COUNTER, 1);
      cubeFacts.add(fact);

      CubeFact fact2 = new CubeFact(ts);
      fact2.addDimensionValue("dummy", "data" + count);
      fact2.addDimensionValue("another", "b-data" + count);
      fact2.addMeasurement("count", MeasureType.COUNTER, 1);
      cubeFacts.add(fact2);
    }
    LOG.info("Going to add {} facts individually {}", cubeFacts.size());
    long atBegin = System.currentTimeMillis();
    for (final CubeFact fact : cubeFacts) {
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          Cube cube = context.getDataset(WebAnalyticsApp.CUBE_NAME_2);
          cube.add(fact);
        }
      });
    }
    LOG.info("Done.. took {} ms", System.currentTimeMillis() - atBegin);
  }
}
