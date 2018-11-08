package mtas.analysis.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/**
 * Constructs pseudo-char filters that replace identifiers by actual documents,
 * by fetching them from disk or URL.
 * <p>
 * This could do with a better name, but we're stuck because the current name used in
 * too many configuration files.
 *
 * TODO: throw me away.
 */
public class MtasCharFilterFactory extends CharFilterFactory implements ResourceLoaderAware {
  private static final Log log = LogFactory.getLog(MtasCharFilterFactory.class);

  private static final String ARGUMENT_TYPE = "type";
  private static final String ARGUMENT_PREFIX = "prefix";
  private static final String ARGUMENT_POSTFIX = "postfix";
  private static final String ARGUMENT_CONFIG = "config";
  private static final String ARGUMENT_DEFAULT = "default";
  private static final String VALUE_TYPE_URL = "url";
  private static final String VALUE_TYPE_FILE = "file";

  private String configArgument;
  private String defaultArgument;
  private String typeArgument;
  private String prefixArgument;
  private String postfixArgument;

  private HashMap<String, MtasConfiguration> configs = null;
  private MtasConfiguration config = null;

  public MtasCharFilterFactory(Map<String, String> args) throws IOException {
    this(args, null);
  }

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

  @Override
  public Reader create(Reader input) {
    return input;
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    init(loader);
  }
}
