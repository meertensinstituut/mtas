package mtas.analysis;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
import mtas.analysis.parser.MtasBasicParser;
import mtas.analysis.token.MtasToken;
import mtas.analysis.token.MtasTokenCollection;
import mtas.analysis.util.MtasConfigException;
import mtas.analysis.util.MtasConfiguration;
import mtas.analysis.util.MtasParserException;
import mtas.codec.payload.MtasPayloadEncoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.AttributeFactory;

/**
 * The Class MtasTokenizer.
 */
public final class MtasTokenizer extends Tokenizer {

  /** The log. */
  private static Log log = LogFactory.getLog(MtasTokenizer.class);

  /** The Constant CONFIGURATION_MTAS. */
  public static final String CONFIGURATION_MTAS = "mtas";

  /** The current position. */
  private int currentPosition = 0;

  /** The encoding flags. */
  private int encodingFlags = MtasPayloadEncoder.ENCODE_DEFAULT;

  /** The parser name. */
  private String parserName = null;

  /** The parser configuration. */
  MtasConfiguration parserConfiguration = null;

  /** The token collection. */
  private MtasTokenCollection tokenCollection;

  /** The term att. */
  private final CharTermAttribute termAtt = addAttribute(
      CharTermAttribute.class);

  /** The offset att. */
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  /** The payload att. */
  private final PayloadAttribute payloadAtt = addAttribute(
      PayloadAttribute.class);

  /** The position increment att. */
  private final PositionIncrementAttribute positionIncrementAtt = addAttribute(
      PositionIncrementAttribute.class);

  /** The token collection iterator. */
  private Iterator<MtasToken<?>> tokenCollectionIterator;

  /**
   * Instantiates a new mtas tokenizer.
   */
  public MtasTokenizer() {
  }

  /**
   * Instantiates a new mtas tokenizer.
   *
   * @param configFileName the config file name
   */
  public MtasTokenizer(String configFileName) {
    readConfigurationFile(configFileName);
  }

  /**
   * Instantiates a new mtas tokenizer.
   *
   * @param config the config
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public MtasTokenizer(MtasConfiguration config) throws IOException {
    processConfiguration(config);
  }

  /**
   * Instantiates a new mtas tokenizer.
   *
   * @param reader the reader
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public MtasTokenizer(InputStream reader) throws IOException {
    processConfiguration(MtasConfiguration.readConfiguration(reader));
  }

  /**
   * Instantiates a new mtas tokenizer.
   *
   * @param factory the factory
   * @param config the config
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public MtasTokenizer(AttributeFactory factory, MtasConfiguration config)
      throws IOException {
    super(factory);
    processConfiguration(config);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.analysis.TokenStream#incrementToken()
   */
  @Override
  public boolean incrementToken() throws IOException {
    clearAttributes();
    MtasToken<?> token;
    Integer positionIncrement;
    MtasPayloadEncoder payloadEncoder;
    if (tokenCollectionIterator == null) {
      return false;
    } else if (tokenCollectionIterator.hasNext()) {
      token = tokenCollectionIterator.next();
      // compute info
      positionIncrement = token.getPositionStart() - currentPosition;
      currentPosition = token.getPositionStart();
      payloadEncoder = new MtasPayloadEncoder(token, encodingFlags);
      // set info
      termAtt.append(token.getValue());
      positionIncrementAtt.setPositionIncrement(positionIncrement);
      offsetAtt.setOffset(token.getOffsetStart(), token.getOffsetEnd());
      payloadAtt.setPayload(payloadEncoder.getPayload());
      return true;
    }
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.analysis.Tokenizer#reset()
   */
  @Override
  public void reset() throws IOException {
    super.reset();
    currentPosition = -1;
    try {
      constructTokenCollection(input);
      tokenCollectionIterator = tokenCollection.iterator();
    } catch (MtasConfigException | MtasParserException e) {
      log.debug(e);
      tokenCollectionIterator = null;
      throw new IOException(
          e.getClass().getSimpleName() + ": " + e.getMessage());
    }
  }

  /**
   * Prints the.
   *
   * @param r the r
   * @throws MtasParserException the mtas parser exception
   */
  public void print(Reader r) throws MtasParserException {
    try {
      setReader(r);
      reset();
      if (tokenCollection != null) {
        tokenCollection.print();
      }
      end();
      close();
    } catch (IOException e) {
      log.error(e);
      throw new MtasParserException(e.getClass() + " : " + e.getMessage());
    }
  }

  /**
   * Gets the list.
   *
   * @param r the r
   * @return the list
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public String[][] getList(Reader r) throws IOException {
    String[][] result = new String[0][];
    try {
      setReader(r);
      reset();
      result = tokenCollection.getList();
      end();
      close();
    } catch (MtasParserException e) {
      log.info(e);
      throw new IOException("can't produce list");
    }
    return result;
  }

  /**
   * Construct token collection.
   *
   * @param reader the reader
   * @throws MtasConfigException the mtas config exception
   * @throws MtasParserException the mtas parser exception
   */
  private void constructTokenCollection(Reader reader)
      throws MtasConfigException, MtasParserException {
    tokenCollection = null;
    try {
      Constructor<?> c = Class.forName(parserName)
          .getDeclaredConstructor(MtasConfiguration.class);
      // try {
      Object p = c.newInstance(parserConfiguration);
      if (p instanceof MtasBasicParser) {
        MtasBasicParser parser = (MtasBasicParser) p;
        tokenCollection = parser.createTokenCollection(reader);
        return;
      } else {
        throw new MtasConfigException("no instance of MtasParser");
      }
    } catch (MtasParserException e) {
      log.debug(e);
      tokenCollection = new MtasTokenCollection();
      throw new MtasParserException(e.getMessage());
    } catch (NoSuchMethodException | InvocationTargetException
        | IllegalAccessException | ClassNotFoundException
        | InstantiationException e) {
      log.debug(e);
      throw new MtasConfigException(
          e.getClass().getName() + " : '" + e.getMessage() + "'");
    }

  }

  /**
   * Read configuration file.
   *
   * @param configFile the config file
   */
  private void readConfigurationFile(String configFile) {
    InputStream is;
    try {
      is = new FileInputStream(configFile);
      processConfiguration(MtasConfiguration.readConfiguration(is));
      is.close();
    } catch (FileNotFoundException e) {
      log.error("Couldn't find " + configFile, e);
    } catch (IOException e) {
      log.error("Couldn't read " + configFile, e);
    }
  }

  /**
   * Process configuration.
   *
   * @param config the config
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void processConfiguration(MtasConfiguration config)
      throws IOException {
    final String NAME_INDEX = "index";
    final String NAME_PARSER = "parser";
    final String NAME_NAME = "name";
    final String VALUE_TRUE = "true";
    final String VALUE_FALSE = "false";
    final String VALUE_0 = "0";
    final String VALUE_1 = "1";
    HashMap<String, Integer> indexEncodingMapper = new HashMap<>();
    indexEncodingMapper.put("payload", MtasPayloadEncoder.ENCODE_PAYLOAD);
    indexEncodingMapper.put("offset", MtasPayloadEncoder.ENCODE_OFFSET);
    indexEncodingMapper.put("realoffset", MtasPayloadEncoder.ENCODE_REALOFFSET);
    indexEncodingMapper.put("parent", MtasPayloadEncoder.ENCODE_PARENT);
    // process
    if (config != null) {
      for (int i = 0; i < config.children.size(); i++) {
        if (config.children.get(i).name.equals(NAME_INDEX)) {
          MtasConfiguration index = config.children.get(i);
          for (int j = 0; j < index.children.size(); j++) {
            if (indexEncodingMapper.containsKey(index.children.get(j).name)) {
              String value = index.children.get(j).attributes.get(NAME_INDEX);
              if ((value.equals(VALUE_TRUE)) || (value.equals(VALUE_1))) {
                encodingFlags |= indexEncodingMapper
                    .get(index.children.get(j).name);
              } else if ((value.equals(VALUE_FALSE))
                  || (value.equals(VALUE_0))) {
                encodingFlags &= ~indexEncodingMapper
                    .get(index.children.get(j).name);
              }
            }
          }
        } else if (config.children.get(i).name.equals(NAME_PARSER)) {
          if (config.children.get(i).attributes.containsKey(NAME_NAME)) {
            parserName = config.children.get(i).attributes.get(NAME_NAME);
            parserConfiguration = config.children.get(i);
          } else {
            throw new IOException("no parser configuration");
          }
        }
      }
    } else {
      throw new IOException("no (valid) configuration");
    }
  }

}