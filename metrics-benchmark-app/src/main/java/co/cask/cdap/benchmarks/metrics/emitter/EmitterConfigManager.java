package co.cask.cdap.benchmarks.metrics.emitter;

import co.cask.cdap.benchmarks.metrics.emitter.EmitterConfig;

/**
 *
 */
public abstract class EmitterConfigManager {

  private static final EmitterConfig DEFAULT_EMITTER_CONFIG = new EmitterConfig(1, 1000);

  /**
   * How long to wait between refreshing {@link EmitterConfig}, in milliseconds.
   */
  private long configRefreshDelay = 3000;
  private long timeLastRefreshedDelay;

  public EmitterConfig config;

  protected abstract EmitterConfig fetchConfig(String key);

  public EmitterConfig getConfig(String key) {
    return fetchConfigWithDefaults(key);
  }

  public EmitterConfig getCachedConfig(String key, long currentTime) {
    if (currentTime - timeLastRefreshedDelay >= configRefreshDelay) {
      config = getConfig(key);
      timeLastRefreshedDelay = currentTime;
    }
    return config;
  }

  private EmitterConfig fetchConfigWithDefaults(String key) {
    EmitterConfig instanceConfig = fetchConfig(key);
    if (instanceConfig != null) {
      return instanceConfig;
    }

    EmitterConfig defaultConfig = fetchConfig(key);
    if (defaultConfig != null) {
      return defaultConfig;
    }

    return DEFAULT_EMITTER_CONFIG;
  }
}
