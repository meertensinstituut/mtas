package mtas.analysis;

import mtas.analysis.parser.MtasParser;
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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Tokenizer that delegates to an MtasParser.
 */
public final class MtasTokenizer extends Tokenizer {
  private static final Log log = LogFactory.getLog(MtasTokenizer.class);

  public static final String CONFIGURATION_MTAS = "mtas";

  public static final String CONFIGURATION_MTAS_INDEX = "index";
  public static final String CONFIGURATION_MTAS_INDEX_ATTRIBUTE = "index";

  public static final String CONFIGURATION_MTAS_PARSER = "parser";
  public static final String CONFIGURATION_MTAS_PARSER_ATTRIBUTE = "name";

  private static final String VALUE_TRUE = "true";
  private static final String VALUE_FALSE = "false";
  private static final String VALUE_0 = "0";
  private static final String VALUE_1 = "1";
  
  private int currentPosition = 0;
  private int encodingFlags = MtasPayloadEncoder.ENCODE_DEFAULT;
  private String parserName = null;
  private MtasConfiguration parserConfiguration = null;
  private MtasTokenCollection tokenCollection;
  private final CharTermAttribute termAtt = addAttribute(
      CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PayloadAttribute payloadAtt = addAttribute(
      PayloadAttribute.class);
  private final PositionIncrementAttribute positionIncrementAtt = addAttribute(
      PositionIncrementAttribute.class);
  private Iterator<MtasToken> tokenCollectionIterator;

  public MtasTokenizer() {
  }

  public MtasTokenizer(final String configFileName) {
    readConfigurationFile(configFileName);
  }

  public MtasTokenizer(final MtasConfiguration config) throws IOException {
    processConfiguration(config);
  }

  public MtasTokenizer(final InputStream reader) throws IOException {
    processConfiguration(MtasConfiguration.readConfiguration(reader));
  }

  public MtasTokenizer(final AttributeFactory factory,
      final MtasConfiguration config) throws IOException {
    super(factory);
    processConfiguration(config);
  }

  @Override
  public boolean incrementToken() throws IOException {
    clearAttributes();
    MtasToken token;
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

  @Override
  public void reset() throws IOException {
    super.reset();
    currentPosition = -1;
    try {
      constructTokenCollection(input);
      tokenCollectionIterator = tokenCollection.iterator();
    } catch (MtasConfigException | MtasParserException e) {      
      tokenCollectionIterator = null;
      throw new IOException(e);
    }
  }

  public void print(final Reader r) throws MtasParserException {
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

  public String[][] getList(final Reader r) throws IOException {
    try {
      setReader(r);
      reset();
      String[][] result = tokenCollection.getList();
      end();
      close();
      return result;
    } catch (MtasParserException e) {
      log.info(e);
      throw new IOException("can't produce list");
    }
  }

  private void constructTokenCollection(final Reader reader)
      throws MtasConfigException, MtasParserException {
    tokenCollection = null;
    try {
      Constructor<?> c = Class.forName(parserName)
          .getDeclaredConstructor(MtasConfiguration.class);      
      Object p = c.newInstance(parserConfiguration);
      if (p instanceof MtasParser) {
        MtasParser parser = (MtasParser) p;
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

  private void readConfigurationFile(final String configFile) {
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

  private void processConfiguration(final MtasConfiguration config)
      throws IOException {
    HashMap<String, Integer> indexEncodingMapper = new HashMap<>();
    indexEncodingMapper.put("payload", MtasPayloadEncoder.ENCODE_PAYLOAD);
    indexEncodingMapper.put("offset", MtasPayloadEncoder.ENCODE_OFFSET);
    indexEncodingMapper.put("realoffset", MtasPayloadEncoder.ENCODE_REALOFFSET);
    indexEncodingMapper.put("parent", MtasPayloadEncoder.ENCODE_PARENT);
    // process
    if (config != null) {
      for (int i = 0; i < config.children.size(); i++) {
        if (config.children.get(i).name.equals(CONFIGURATION_MTAS_INDEX)) {
          MtasConfiguration index = config.children.get(i);
          for (int j = 0; j < index.children.size(); j++) {
            if (indexEncodingMapper.containsKey(index.children.get(j).name)) {
              String value = index.children.get(j).attributes.get(CONFIGURATION_MTAS_INDEX_ATTRIBUTE);
              if ((value.equals(VALUE_TRUE)) || (value.equals(VALUE_1))) {
                encodingFlags |= indexEncodingMapper
                    .get(index.children.get(j).name);
              } else if ((value.equals(VALUE_FALSE)) || (value.equals(VALUE_0))) {
                encodingFlags &= ~indexEncodingMapper
                    .get(index.children.get(j).name);
              }
            }
          }
        } else if (config.children.get(i).name.equals(CONFIGURATION_MTAS_PARSER)) {
          if (config.children.get(i).attributes.containsKey(CONFIGURATION_MTAS_PARSER_ATTRIBUTE)) {
            parserName = config.children.get(i).attributes.get(CONFIGURATION_MTAS_PARSER_ATTRIBUTE);
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

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final MtasTokenizer that = (MtasTokenizer) obj;
    return super.equals(that);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
