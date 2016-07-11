package mtas.analysis.util;

import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.solr.core.SolrResourceLoader;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/**
 * A factory for creating MtasCharFilter objects.
 */
public class MtasCharFilterFactory extends CharFilterFactory implements ResourceLoaderAware {

  public static String ARGUMENT_TYPE = "type";
  public static String ARGUMENT_PREFIX = "prefix";
  public static String ARGUMENT_CONFIG = "config";
  public static String ARGUMENT_DEFAULT = "default";

  public static String VALUE_TYPE_URL = "url";
  public static String VALUE_TYPE_FILE = "file";

  /** The available configurations. */
  String configArgument;

  /** The default configuration. */
  String defaultArgument;

  /** The type. */
  String typeArgument;

  /** The prefix. */
  String prefixArgument;

  private HashMap<String, MtasConfiguration> configs = null;
  private MtasConfiguration config = null;
  
  public MtasCharFilterFactory(Map<String, String> args) throws IOException {
    this(args, null);
  }

  /**
   * Instantiates a new mtas char filter factory.
   *
   * @param args
   *          the args
   * @throws IOException
   */
  public MtasCharFilterFactory(Map<String, String> args,
      SolrResourceLoader resourceLoader) throws IOException {
    super(args);
    typeArgument = get(args, ARGUMENT_TYPE);
    prefixArgument = get(args, ARGUMENT_PREFIX);
    configArgument = get(args, ARGUMENT_CONFIG);
    defaultArgument = get(args, ARGUMENT_DEFAULT);
    if (typeArgument != null && configArgument != null) {
      throw new IOException(this.getClass().getName() + " can't have both "
          + ARGUMENT_TYPE + " and " + ARGUMENT_CONFIG);
    } else if (typeArgument == null && prefixArgument != null) {
      throw new IOException(this.getClass().getName() + " can't have "
          + ARGUMENT_PREFIX + " without " + ARGUMENT_TYPE);
    } else if (configArgument == null && defaultArgument != null) {
      throw new IOException(this.getClass().getName() + " can't have "
          + ARGUMENT_DEFAULT + " without " + ARGUMENT_CONFIG);
    } else if (typeArgument == null && configArgument == null) {
      throw new IOException(this.getClass().getName() + " should have "
          + ARGUMENT_TYPE + " or " + ARGUMENT_CONFIG);
    }
    init(resourceLoader);
  }

  private void init(ResourceLoader resourceLoader) throws IOException {
    if (config == null && configs == null) {
      if (typeArgument == null && configArgument == null) {
        throw new IOException("no configuration");
      } else {
        if (typeArgument != null) {
          config = new MtasConfiguration();
          config.attributes.put(MtasConfiguration.CHARFILTER_CONFIGURATION_TYPE,
              typeArgument);
          config.attributes.put(
              MtasConfiguration.CHARFILTER_CONFIGURATION_PREFIX,
              prefixArgument);
        }
        if (configArgument != null) {
          if (resourceLoader != null) {
            try {
              configs = MtasConfiguration.readMtasCharFilterConfigurations(
                  resourceLoader, configArgument);
            } catch (IOException e) {
              throw new IOException("problem loading configurations from "
                  + configArgument + ": " + e.getMessage());
            }
          } 
        }
      }
    }
  }

  @Override
  public Reader create(Reader input) {
    String configuration = null;
    try {
      return create(input, configuration);
    } catch (IOException e) {
      return null;
    }
  }

  public Reader create(Reader input, String configuration) throws IOException {
    if (configs != null && configs.size() > 0) {
      if (configuration == null && defaultArgument == null) {
        throw new IOException("no (default)configuration");
      } else if (configuration == null) {
        if (configs.get(defaultArgument) != null) {
          return create(input, configs.get(defaultArgument));
        } else {
          throw new IOException(
              "default configuration " + defaultArgument + " not available");
        }
      } else {
        MtasConfiguration config = configs.get(configuration);
        if (config == null) {
          if (defaultArgument != null) {
            if (configs.get(defaultArgument) != null) {
              return create(input, configs.get(defaultArgument));
            } else {
              throw new IOException("configuration " + configuration
                  + " not found and default configuration " + defaultArgument
                  + " not available");
            }
          } else {
            throw new IOException("configuration " + configuration
                + " not available and no default configuration");
          }
        } else {
          return create(input, config);
        }
      }
    } else if (config != null) {
      return create(input, config);
    } else {
      throw new IOException("no configuration");
    }

  }

  public Reader create(Reader input, MtasConfiguration config) {
    MtasFetchData fetchData = new MtasFetchData(input);
    if (config.attributes
        .containsKey(MtasConfiguration.CHARFILTER_CONFIGURATION_TYPE)) {
      if (config.attributes.get(MtasConfiguration.CHARFILTER_CONFIGURATION_TYPE)
          .equals(VALUE_TYPE_URL)) {
        try {
          return fetchData.getUrl(config.attributes
              .get(MtasConfiguration.CHARFILTER_CONFIGURATION_PREFIX));
        } catch (MtasParserException e) {
          return null;
        }
      } else if (config.attributes
          .get(MtasConfiguration.CHARFILTER_CONFIGURATION_TYPE)
          .equals(VALUE_TYPE_FILE)) {
        try {
          return fetchData.getFile(config.attributes
              .get(MtasConfiguration.CHARFILTER_CONFIGURATION_PREFIX));
        } catch (MtasParserException e) {
          return null;
        }
      } else {
        return fetchData.getDefault();
      }
    } else {
      return fetchData.getDefault();
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    init(loader);
  }

}
