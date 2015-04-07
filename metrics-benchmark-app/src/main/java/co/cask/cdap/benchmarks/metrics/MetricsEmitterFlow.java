package co.cask.cdap.benchmarks.metrics;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.benchmarks.metrics.emitter.EmitterConfig;
import co.cask.cdap.benchmarks.metrics.emitter.EmitterConfigManager;

import java.util.concurrent.TimeUnit;

/**
 * Emits metrics when {@link co.cask.cdap.benchmarks.metrics.emitter.EmitEvent}s are received from the events stream.
 */
public final class MetricsEmitterFlow implements Flow {

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("MetricsEmitterFlow")
      .setDescription("Emits metrics")
      .withFlowlets()
      .add("emitter", new MetricsEmitter())
      .add("foo", new Foo())
      .connect()
      .fromStream("foo").to("foo")
      .build();
  }

  public final class Foo extends AbstractFlowlet {
    @ProcessInput
    public void process(StreamEvent event) {

    }
  }

  /**
   * Emits metrics.
   */
  public final class MetricsEmitter extends AbstractFlowlet {

    @UseDataSet("configs")
    private ObjectStore<EmitterConfig> configs;

    private Metrics metrics;
    private EmitterConfigManager configManager;

    @Override
    public void initialize(FlowletContext context) throws Exception {
      super.initialize(context);

      configManager = new EmitterConfigManager() {
        @Override
        protected EmitterConfig fetchConfig(String key) {
          return configs.read(key);
        }
      };
    }

    @Tick(delay = 1L, unit = TimeUnit.SECONDS)
    public void generate() {
      long currentTime = System.currentTimeMillis();
      EmitterConfig config = configManager.getCachedConfig(currentTime);
      for (int i = 0; i < config.getBatchSize(); i++) {
        metrics.gauge("metric-" + i, currentTime);
      }
    }

  }
}
