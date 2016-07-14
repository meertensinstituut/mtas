package mtas.analysis.util;

import mtas.analysis.MtasTokenizer;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;
import org.apache.solr.core.SolrResourceLoader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A factory for creating MtasTokenizer objects.
 */
public class MtasTokenizerFactory extends TokenizerFactory
    implements ResourceLoaderAware {

  /** The argument configfile. */
  public static String ARGUMENT_CONFIGFILE = "configFile";
  
  /** The argument config. */
  public static String ARGUMENT_CONFIG = "config";
  
  /** The argument default. */
  public static String ARGUMENT_DEFAULT = "default";

  /** The config argument. */
  private String configArgument;

  /** The default argument. */
  private String defaultArgument;

  /** The config file argument. */
  private String configFileArgument;

  /** The configs. */
  private HashMap<String, MtasConfiguration> configs = null;
  
  /** The config. */
  private MtasConfiguration config = null;

  /**
   * Instantiates a new mtas tokenizer factory.
   *
   * @param args the args
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public MtasTokenizerFactory(Map<String, String> args) throws IOException {
    this(args, null);
  }

  /**
   * Instantiates a new mtas tokenizer factory.
   *
   * @param args the args
   * @param resourceLoader the resource loader
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public MtasTokenizerFactory(Map<String, String> args,
      SolrResourceLoader resourceLoader) throws IOException {
    super(args);
    configFileArgument = get(args, ARGUMENT_CONFIGFILE);
    configArgument = get(args, ARGUMENT_CONFIG);
    defaultArgument = get(args, ARGUMENT_DEFAULT);
    if (configFileArgument != null && configArgument != null) {
      throw new IOException(this.getClass().getName() + " can't have both "
          + ARGUMENT_CONFIGFILE + " and " + ARGUMENT_CONFIG);
    } else if (configArgument == null && defaultArgument != null) {
      throw new IOException(this.getClass().getName() + " can't have "
          + ARGUMENT_DEFAULT + " without " + ARGUMENT_CONFIG);
    } else if (configFileArgument == null && configArgument == null) {
      throw new IOException(this.getClass().getName() + " should have "
          + ARGUMENT_CONFIGFILE + " or " + ARGUMENT_CONFIG);
    }
    init(resourceLoader);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.analysis.util.TokenizerFactory#create(org.apache.lucene.
   * util.AttributeFactory)
   */
  @Override
  public MtasTokenizer create(AttributeFactory factory) {
    MtasTokenizer tokenizer = null;
    try {
      tokenizer = create(factory, null);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return tokenizer;
  }

  /**
   * Creates the.
   *
   * @param configuration the configuration
   * @return the mtas tokenizer
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public MtasTokenizer create(String configuration) throws IOException {
    return create(TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY, configuration);
  }

  /**
   * Creates the.
   *
   * @param factory the factory
   * @param configuration the configuration
   * @return the mtas tokenizer
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public MtasTokenizer create(AttributeFactory factory, String configuration)
      throws IOException {
    if (configs != null && configs.size() > 0) {
      if (configuration == null && defaultArgument == null) {
        throw new IOException("no (default)configuration");
      } else if (configuration == null) {
        if (configs.get(defaultArgument) != null) {
          return new MtasTokenizer(factory, configs.get(defaultArgument));
        } else {
          throw new IOException(
              "default configuration " + defaultArgument + " not available");
        }
      } else {
        MtasConfiguration config = configs.get(configuration);
        if (config == null) {
          if (defaultArgument != null) {
            if (configs.get(defaultArgument) != null) {
              return new MtasTokenizer(factory, configs.get(defaultArgument));
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
          return new MtasTokenizer(factory, config);
        }
      }
    } else if (config != null) {
      return new MtasTokenizer(factory, config);
    } else {
      throw new IOException("no configuration");
    }
  }

  /**
   * Inits the.
   *
   * @param resourceLoader the resource loader
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void init(ResourceLoader resourceLoader) throws IOException {
    if (config == null && configs == null) {
      if (resourceLoader == null) {
        return;
      } else if (configFileArgument == null && configArgument == null) {
        throw new IOException("no configuration");
      } else {
        if (configFileArgument != null) {
          try {
            config = MtasConfiguration.readConfiguration(
                resourceLoader.openResource(configFileArgument));
          } catch (IOException e) {
            throw new IOException("Problem loading configuration from "
                + configFileArgument + ": " + e.getMessage());
          }
        }
        if (configArgument != null) {
          try {
            configs = MtasConfiguration.readMtasTokenizerConfigurations(resourceLoader,
                configArgument);
          } catch (IOException e) {
            throw new IOException("Problem loading configurations from "
                + configArgument + ": " + e.getMessage());
          }
        }
      }
    }
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.analysis.util.ResourceLoaderAware#inform(org.apache.lucene.analysis.util.ResourceLoader)
   */
  @Override
  public void inform(ResourceLoader loader) throws IOException {
    init(loader);
  }

}
