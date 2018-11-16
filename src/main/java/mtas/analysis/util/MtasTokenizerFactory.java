package mtas.analysis.util;

import mtas.analysis.MtasTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;

import javax.xml.stream.XMLStreamException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;

/**
 * A factory for creating MtasTokenizer objects.
 */
public class MtasTokenizerFactory extends TokenizerFactory
    implements ResourceLoaderAware {
  public static final String ARGUMENT_CONFIGFILE = "configFile";
  public static final String ARGUMENT_CONFIG = "config";
  public static final String ARGUMENT_PARSER = "parser";
  public static final String ARGUMENT_PARSER_ARGS = "parserArgs";

  private String configArgument;
  private String configFileArgument;
  private String analyzerArgument;
  private String analyzerArgumentParserArgs;
  private ResourceLoader loader;

  public MtasTokenizerFactory(Map<String, String> args) throws IOException {
    super(args);
    configFileArgument = get(args, ARGUMENT_CONFIGFILE);
    configArgument = get(args, ARGUMENT_CONFIG);
    analyzerArgument = get(args, ARGUMENT_PARSER);
    analyzerArgumentParserArgs = get(args, ARGUMENT_PARSER_ARGS);
    int numberOfArgs = 0;
    numberOfArgs += (configFileArgument == null) ? 0 : 1;
    numberOfArgs += (configArgument == null) ? 0 : 1;
    numberOfArgs += (analyzerArgument == null) ? 0 : 1;
    
    if (numberOfArgs>1) {
      throw new IOException(this.getClass().getName() + " can't have multiple of "
          + ARGUMENT_CONFIGFILE + ", " + ARGUMENT_CONFIG+" AND "+ARGUMENT_PARSER);
    } else if (numberOfArgs==0) {
      throw new IOException(this.getClass().getName() + " should have "
          + ARGUMENT_CONFIGFILE + " or " + ARGUMENT_CONFIG+" or "+ARGUMENT_PARSER);
    }

    if (configFileArgument == null && configArgument == null && analyzerArgument == null) {
      throw new IOException("no configuration");
    }
  }

  @Override
  public MtasTokenizer create(AttributeFactory factory) {
    try {
      return create(factory, null);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public MtasTokenizer create(String configuration) throws IOException {
    return create(TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY, configuration);
  }

  public MtasTokenizer create(AttributeFactory factory, String collection) throws IOException {
    if (loader == null) {
      throw new IllegalStateException("loader == null, inform not called?");
    }

    if (configFileArgument != null) {
      return new MtasTokenizer(factory, loadConfig(configFileArgument));
    }

    Configuration config = null;
    if (configArgument != null) {
      String path = "mtasconf/" + collection + ".xml";
      config = loadConfig(path);
    }
    if (analyzerArgument != null) {
      Configuration subConfig = new Configuration("parser", null,
        "name", analyzerArgument,
        ARGUMENT_PARSER_ARGS, analyzerArgumentParserArgs);
      config = new Configuration(subConfig);
    }
    return new MtasTokenizer(factory, Objects.requireNonNull(config));
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    this.loader = Objects.requireNonNull(loader);
  }

  // Configurations are loaded on demand by create, since inform doesn't know the
  // collection name. It's impossible to load all configurations without knowing their
  // names, since ZooKeeper ResourceLoaders don't give access to directories.
  // This doesn't matter for performance, since tokenizers are long-lived; looks like
  // Solr/Lucene caches them.
  private Configuration loadConfig(String path) {
    try {
      return Configuration.read(loader.openResource(path));
    } catch (IOException e) {
      throw new UncheckedIOException("Problem loading configuration from resource " + path, e);
    } catch (XMLStreamException e) {
      throw new RuntimeException("Problem loading configuration from resource " + path, e);
    }
  }
}
