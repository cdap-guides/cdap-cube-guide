package co.cask.cdap.benchmarks.metrics;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.benchmarks.metrics.emitter.CommonSetEmitter;
import co.cask.cdap.benchmarks.metrics.emitter.Emitter;
import co.cask.cdap.benchmarks.metrics.emitter.EmitterConfig;
import co.cask.cdap.benchmarks.metrics.emitter.EmitterConfigManager;
import com.google.common.base.Throwables;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Emits metrics.
 */
public final class MetricsEmitter extends AbstractWorker {

  public static final String NAME = "MetricsEmitter";

  private Metrics metrics;
  private EmitterConfigManager configManager;
  private String instanceName;

  private Emitter emitter = new CommonSetEmitter();

  @Override
  protected void configure() {
    setName(NAME);
  }

  @Override
  public void initialize(WorkerContext context) throws Exception {
    super.initialize(context);
    this.instanceName = "emitter-" + context.getInstanceId();
    this.configManager = new EmitterConfigManager() {
      @Override
      protected EmitterConfig fetchConfig(final String key) {
        final AtomicReference<EmitterConfig> result = new AtomicReference<EmitterConfig>();
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext datasetContext) throws Exception {
            ObjectStore<EmitterConfig> delays = datasetContext.getDataset("configs");
            result.set(delays.read(key));

          }
        });
        return result.get();
      }
    };
  }

  @Override
  public void run() {
    while(true) {
      long currentTime = System.currentTimeMillis();
      EmitterConfig config = configManager.getCachedConfig(instanceName, currentTime);
      emitter.emit(metrics, currentTime, instanceName, config);

      try {
        Thread.sleep(config.getDelayMs());
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
