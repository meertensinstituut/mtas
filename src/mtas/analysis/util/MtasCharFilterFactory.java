package mtas.analysis.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
public class MtasCharFilterFactory extends CharFilterFactory
    implements ResourceLoaderAware {

  /** The Constant log. */
  private static final Log log = LogFactory.getLog(MtasCharFilterFactory.class);

  /** The Constant ARGUMENT_TYPE. */
  public static final String ARGUMENT_TYPE = "type";

  /** The Constant ARGUMENT_PREFIX. */
  public static final String ARGUMENT_PREFIX = "prefix";

  /** The Constant ARGUMENT_POSTFIX. */
  public static final String ARGUMENT_POSTFIX = "postfix";

  /** The Constant ARGUMENT_CONFIG. */
  public static final String ARGUMENT_CONFIG = "config";

  /** The Constant ARGUMENT_DEFAULT. */
  public static final String ARGUMENT_DEFAULT = "default";

  /** The Constant VALUE_TYPE_URL. */
  public static final String VALUE_TYPE_URL = "url";

  /** The Constant VALUE_TYPE_FILE. */
  public static final String VALUE_TYPE_FILE = "file";

  /** The config argument. */
  String configArgument;

  /** The default argument. */
  String defaultArgument;

  /** The type argument. */
  String typeArgument;

  /** The prefix argument. */
  String prefixArgument;

  /** The postfix argument. */
  String postfixArgument;

  /** The configs. */
  private HashMap<String, MtasConfiguration> configs = null;

  /** The config. */
  private MtasConfiguration config = null;

  /**
   * Instantiates a new mtas char filter factory.
   *
   * @param args the args
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public MtasCharFilterFactory(Map<String, String> args) throws IOException {
    this(args, null);
  }

  /**
   * Instantiates a new mtas char filter factory.
   *
   * @param args the args
   * @param resourceLoader the resource loader
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public MtasCharFilterFactory(Map<String, String> args,
      ResourceLoader resourceLoader) throws IOException {
    super(args);
    typeArgument = get(args, ARGUMENT_TYPE);
    prefixArgument = get(args, ARGUMENT_PREFIX);
    postfixArgument = get(args, ARGUMENT_POSTFIX);
    configArgument = get(args, ARGUMENT_CONFIG);
    defaultArgument = get(args, ARGUMENT_DEFAULT);
    if (typeArgument != null && configArgument != null) {
      throw new IOException(this.getClass().getName() + " can't have both "
          + ARGUMENT_TYPE + " and " + ARGUMENT_CONFIG);
    } else if (typeArgument == null && prefixArgument != null) {
      throw new IOException(this.getClass().getName() + " can't have "
          + ARGUMENT_PREFIX + " without " + ARGUMENT_TYPE);
    } else if (typeArgument == null && postfixArgument != null) {
      throw new IOException(this.getClass().getName() + " can't have "
          + ARGUMENT_POSTFIX + " without " + ARGUMENT_TYPE);
    } else if (configArgument == null && defaultArgument != null) {
      throw new IOException(this.getClass().getName() + " can't have "
          + ARGUMENT_DEFAULT + " without " + ARGUMENT_CONFIG);
    } else if (typeArgument == null && configArgument == null) {
      throw new IOException(this.getClass().getName() + " should have "
          + ARGUMENT_TYPE + " or " + ARGUMENT_CONFIG);
    }
    init(resourceLoader);
  }

  /**
   * Inits the.
   *
   * @param resourceLoader the resource loader
   * @throws IOException Signals that an I/O exception has occurred.
   */
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
          config.attributes.put(
              MtasConfiguration.CHARFILTER_CONFIGURATION_POSTFIX,
              postfixArgument);
        }
        if (configArgument != null) {
          if (resourceLoader != null) {
            try {
              configs = MtasConfiguration.readMtasCharFilterConfigurations(
                  resourceLoader, configArgument);
            } catch (IOException e) {
              throw new IOException(
                  "problem loading configurations from " + configArgument, e);
            }
          }
        }
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.analysis.util.CharFilterFactory#create(java.io.Reader)
   */
  @Override
  public Reader create(Reader input) {
    String configuration = null;
    try {
      return create(input, configuration);
    } catch (IOException e) {
      log.debug(e);
      return null;
    }
  }

  /**
   * Creates the.
   *
   * @param input the input
   * @param configuration the configuration
   * @return the reader
   * @throws IOException Signals that an I/O exception has occurred.
   */
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

  /**
   * Creates the.
   *
   * @param input the input
   * @param config the config
   * @return the reader
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public Reader create(Reader input, MtasConfiguration config)
      throws IOException {
    MtasFetchData fetchData = new MtasFetchData(input);
    if (config.attributes
        .containsKey(MtasConfiguration.CHARFILTER_CONFIGURATION_TYPE)) {
      if (config.attributes.get(MtasConfiguration.CHARFILTER_CONFIGURATION_TYPE)
          .equals(VALUE_TYPE_URL)) {
        try {
          return fetchData.getUrl(
              config.attributes
                  .get(MtasConfiguration.CHARFILTER_CONFIGURATION_PREFIX),
              config.attributes
                  .get(MtasConfiguration.CHARFILTER_CONFIGURATION_POSTFIX));
        } catch (MtasParserException e) {
          log.debug(e);
          throw new IOException(e.getMessage());
        }
      } else if (config.attributes
          .get(MtasConfiguration.CHARFILTER_CONFIGURATION_TYPE)
          .equals(VALUE_TYPE_FILE)) {
        try {
          return fetchData.getFile(
              config.attributes
                  .get(MtasConfiguration.CHARFILTER_CONFIGURATION_PREFIX),
              config.attributes
                  .get(MtasConfiguration.CHARFILTER_CONFIGURATION_POSTFIX));
        } catch (MtasParserException e) {
          throw new IOException(e);
        }
      } else {
        return fetchData.getDefault();
      }
    } else {
      return fetchData.getDefault();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.analysis.util.ResourceLoaderAware#inform(org.apache.
   * lucene.analysis.util.ResourceLoader)
   */
  @Override
  public void inform(ResourceLoader loader) throws IOException {
    init(loader);
  }

}
