package mtas.analysis.util;

import mtas.analysis.MtasTokenizer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A factory for creating MtasTokenizer objects.
 */
public class MtasTokenizerFactory extends TokenizerFactory
    implements ResourceLoaderAware {
  private static final Log log = LogFactory.getLog(MtasTokenizerFactory.class);

  public static final String ARGUMENT_CONFIGFILE = "configFile";
  public static final String ARGUMENT_CONFIG = "config";
  public static final String ARGUMENT_PARSER = "parser";
  public static final String ARGUMENT_PARSER_ARGS = "parserArgs";
  public static final String ARGUMENT_DEFAULT = "default";

  private String configArgument;
  private String defaultArgument;
  private String configFileArgument;
  private String analyzerArgument;
  private String analyzerArgumentParserArgs;
  private HashMap<String, MtasConfiguration> configs = null;
  private MtasConfiguration config = null;

  public MtasTokenizerFactory(Map<String, String> args) throws IOException {
    super(args);
    configFileArgument = get(args, ARGUMENT_CONFIGFILE);
    configArgument = get(args, ARGUMENT_CONFIG);
    analyzerArgument = get(args, ARGUMENT_PARSER);
    analyzerArgumentParserArgs = get(args, ARGUMENT_PARSER_ARGS);
    defaultArgument = get(args, ARGUMENT_DEFAULT);
    int numberOfArgs = 0;
    numberOfArgs += (configFileArgument == null) ? 0 : 1;
    numberOfArgs += (configArgument == null) ? 0 : 1;
    numberOfArgs += (analyzerArgument == null) ? 0 : 1;
    
    if (numberOfArgs>1) {
      throw new IOException(this.getClass().getName() + " can't have multiple of "
          + ARGUMENT_CONFIGFILE + ", " + ARGUMENT_CONFIG+" AND "+ARGUMENT_PARSER);
    } else if (configArgument == null && defaultArgument != null) {
      throw new IOException(this.getClass().getName() + " can't have "
          + ARGUMENT_DEFAULT + " without " + ARGUMENT_CONFIG);
    } else if (numberOfArgs==0) {
      throw new IOException(this.getClass().getName() + " should have "
          + ARGUMENT_CONFIGFILE + " or " + ARGUMENT_CONFIG+" or "+ARGUMENT_PARSER);
    }
  }

  @Override
  public MtasTokenizer create(AttributeFactory factory) {
    MtasTokenizer tokenizer = null;
    try {
      tokenizer = create(factory, null);
    } catch (IOException e) {
      log.error(e);
    }
    return tokenizer;
  }

  public MtasTokenizer create(String configuration) throws IOException {
    return create(TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY, configuration);
  }
  
  public MtasTokenizer create(String configuration, String defaultConfiguration) throws IOException {
    return create(TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY, configuration, defaultConfiguration);
  }
  
  public MtasTokenizer create(AttributeFactory factory, String configuration) throws IOException {
    return create(factory, configuration, null);
  }

  public MtasTokenizer create(AttributeFactory factory, String configuration, String defaultConfiguration)
    throws IOException {
    if (defaultConfiguration == null) {
      defaultConfiguration = defaultArgument;
    }
    if (configs != null && configs.size() > 0) {
      if (configuration == null && defaultConfiguration == null) {
        throw new IOException("no (default)configuration");
      } else if (configuration == null) {
        if (configs.get(defaultConfiguration) == null) {
          throw new IOException("default configuration " + defaultConfiguration + " not available");
        }
        return new MtasTokenizer(factory, configs.get(defaultConfiguration));
      } else {
        MtasConfiguration config = configs.get(configuration);
        if (config != null) {
          return new MtasTokenizer(factory, config);
        }
        if (defaultConfiguration == null) {
          throw new IOException("configuration " + configuration
            + " not available and no default configuration");
        }
        if (configs.get(defaultConfiguration) == null) {
          throw new IOException("configuration " + configuration
            + " not found and default configuration " + defaultConfiguration
            + " not available");
        }
        return new MtasTokenizer(factory, configs.get(defaultConfiguration));
      }
    } else if (config != null) {
      return new MtasTokenizer(factory, config);
    } else {
      throw new IOException("no configuration");
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    if (config != null || configs != null || loader == null) {
      return;
    }
    if (configFileArgument == null && configArgument == null && analyzerArgument == null) {
      throw new IOException("no configuration");
    }
    if (configFileArgument != null) {
      try {
        config = MtasConfiguration.readConfiguration(
          loader.openResource(configFileArgument));
      } catch (IOException e) {
        throw new IOException(
          "Problem loading configuration from " + configFileArgument, e);
      }
    }
    if (configArgument != null) {
      try {
        configs = MtasConfiguration.readMtasTokenizerConfigurations(
          loader, configArgument);
      } catch (IOException e) {
        throw new IOException(
          "Problem loading configurations from " + configArgument, e);
      }
    }
    if (analyzerArgument != null) {
      configs = null;
      config = new MtasConfiguration();
      MtasConfiguration subConfig = new MtasConfiguration();
      subConfig.name = "parser";
      subConfig.attributes.put("name", analyzerArgument);
      subConfig.attributes.put(ARGUMENT_PARSER_ARGS, analyzerArgumentParserArgs);
      config.children.add(subConfig);
    }
  }
}
