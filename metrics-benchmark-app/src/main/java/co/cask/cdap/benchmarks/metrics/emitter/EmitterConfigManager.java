package co.cask.cdap.benchmarks.metrics.emitter;

/**
 *
 */
public abstract class EmitterConfigManager {

  private static final EmitterConfig DEFAULT_EMITTER_CONFIG = new EmitterConfig(10);

  /**
   * How long to wait between refreshing {@link EmitterConfig}, in milliseconds.
   */
  private long configRefreshDelay = 3000;
  private long timeLastRefreshedDelay;

  public EmitterConfig config;

  protected abstract EmitterConfig fetchConfig(String key);

  public EmitterConfig getConfig() {
    return fetchConfigWithDefaults();
  }

  public EmitterConfig getCachedConfig(long currentTime) {
    if (currentTime - timeLastRefreshedDelay >= configRefreshDelay) {
      config = getConfig();
      timeLastRefreshedDelay = currentTime;
    }
    return config;
  }

  private EmitterConfig fetchConfigWithDefaults() {
    EmitterConfig defaultConfig = fetchConfig("default");
    if (defaultConfig != null) {
      return defaultConfig;
    }

    return DEFAULT_EMITTER_CONFIG;
  }
}
