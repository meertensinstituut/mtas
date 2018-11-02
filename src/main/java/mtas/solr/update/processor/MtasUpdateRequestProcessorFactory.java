package mtas.solr.update.processor;

import mtas.analysis.MtasTokenizer;
import mtas.analysis.util.MtasCharFilterFactory;
import mtas.analysis.util.MtasTokenizerFactory;
import mtas.codec.util.CodecUtil;
import mtas.solr.schema.MtasPreAnalyzedField;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class MtasUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory {
  private static final Log log = LogFactory
      .getLog(MtasUpdateRequestProcessorFactory.class);

  private MtasUpdateRequestProcessorConfig config = null;

  @Override
  @SuppressWarnings("rawtypes")
  public void init(NamedList args) {
    super.init(args);
  }

  @SuppressWarnings("unchecked")
  private void init(SolrQueryRequest req) throws Exception {
    if (config != null) {
      return;
    }

    // initialise
    config = new MtasUpdateRequestProcessorConfig();
    // required info
    Map<String, FieldType> fieldTypes = req.getSchema().getFieldTypes();
    Map<String, SchemaField> fields = req.getSchema().getFields();
    SolrResourceLoader resourceLoader = req.getCore().getSolrConfig()
                                           .getResourceLoader();

    // check fieldTypes
    for (Entry<String, FieldType> entry : fieldTypes.entrySet()) {
      String key = entry.getKey();
      FieldType value = entry.getValue();

      // only for MtasPreAnalyzedField
      if (value instanceof MtasPreAnalyzedField) {
        MtasPreAnalyzedField mpaf = (MtasPreAnalyzedField) value;
        config.fieldTypeDefaultConfiguration.put(key, mpaf.defaultConfiguration);
        config.fieldTypeConfigurationFromField.put(key, mpaf.configurationFromField);
        config.fieldTypeNumberOfTokensField.put(key, mpaf.setNumberOfTokens);
        config.fieldTypeNumberOfPositionsField.put(key, mpaf.setNumberOfPositions);
        config.fieldTypeSizeField.put(key, mpaf.setSize);
        config.fieldTypeErrorField.put(key, mpaf.setError);
        config.fieldTypePrefixField.put(key, mpaf.setPrefix);
        config.fieldTypePrefixNumbersFieldPrefix.put(key, mpaf.setPrefixNumbers);
        if (mpaf.followIndexAnalyzer == null
          || !fieldTypes.containsKey(mpaf.followIndexAnalyzer)) {
          throw new RuntimeException(
            key + " can't follow " + mpaf.followIndexAnalyzer);
        } else {
          FieldType fieldType = fieldTypes.get(mpaf.followIndexAnalyzer);
          SimpleOrderedMap<?> analyzer = null;
          Object tmpObj1 = fieldType.getNamedPropertyValues(false)
                                    .get(FieldType.INDEX_ANALYZER);
          if (tmpObj1 != null && tmpObj1 instanceof SimpleOrderedMap) {
            analyzer = (SimpleOrderedMap<?>) tmpObj1;
          }
          if (analyzer == null) {
            Object tmpObj2 = fieldType.getNamedPropertyValues(false)
                                      .get(FieldType.ANALYZER);
            if (tmpObj2 != null && tmpObj2 instanceof SimpleOrderedMap) {
              analyzer = (SimpleOrderedMap<?>) tmpObj2;
            }
          }
          if (analyzer == null) {
            throw new RuntimeException("no analyzer");
          }

          // charfilters
          ArrayList<SimpleOrderedMap<Object>> charFilters;
          SimpleOrderedMap<Object> configTokenizer;
          charFilters = (ArrayList<SimpleOrderedMap<Object>>) analyzer.findRecursive(FieldType.CHAR_FILTERS);
          configTokenizer = (SimpleOrderedMap<Object>) analyzer.findRecursive(FieldType.TOKENIZER);

          if (charFilters == null || charFilters.isEmpty()) {
            config.fieldTypeCharFilterFactories.put(key, null);
          } else {
            CharFilterFactory[] charFilterFactories = new CharFilterFactory[charFilters.size()];
            int number = 0;
            for (SimpleOrderedMap<Object> configCharFilter : charFilters) {
              String className = null;
              Map<String, String> args = new HashMap<>();
              // get className and args
              for (Entry<String, Object> obj : configCharFilter) {
                if (obj.getValue() instanceof String) {
                  String name = (String) obj.getValue();
                  if (obj.getKey().equals(FieldType.CLASS_NAME)) {
                    className = name;
                  } else {
                    args.put(obj.getKey(), name);
                  }
                }
              }
              if (className == null) {
                throw new RuntimeException("no className");
              }
              Class<?> cls = Class.forName(className);
              if (cls.isAssignableFrom(MtasCharFilterFactory.class)) {
                Object obj = cls.getConstructor(Map.class, ResourceLoader.class)
                                .newInstance(args, resourceLoader);
                if (!(obj instanceof MtasCharFilterFactory)) {
                  throw new RuntimeException(className + " is no MtasCharFilterFactory");
                }
                charFilterFactories[number] = (MtasCharFilterFactory) obj;
                number++;
              } else {
                Object obj = cls.getConstructor(Map.class).newInstance(args);
                if (!(obj instanceof CharFilterFactory)) {
                  throw new RuntimeException(className + " is no CharFilterFactory");
                }
                charFilterFactories[number] = (CharFilterFactory) obj;
                number++;
              }
            }
            config.fieldTypeCharFilterFactories.put(key, charFilterFactories);
          }

          if (configTokenizer != null) {
            String className = null;
            Map<String, String> args = new HashMap<>();
            // get className and args
            for (Entry<String, Object> obj : configTokenizer) {
              if (obj.getValue() instanceof String) {
                if (obj.getKey().equals(FieldType.CLASS_NAME)) {
                  className = (String) obj.getValue();
                } else {
                  args.put(obj.getKey(), (String) obj.getValue());
                }
              }
            }
            if (className == null) {
              throw new RuntimeException("no className");
            }
            Object obj = Class.forName(className).getConstructor(Map.class, ResourceLoader.class)
                              .newInstance(args, resourceLoader);
            if (obj instanceof MtasTokenizerFactory) {
              config.fieldTypeTokenizerFactory.put(key, (MtasTokenizerFactory) obj);
            } else {
              throw new RuntimeException(className + " is no MtasTokenizerFactory");
            }
          }
        }
      }
    }

    for (Entry<String, SchemaField> entry : fields.entrySet()) {
      FieldType type = entry.getValue().getType();
      if (type != null && config.fieldTypeTokenizerFactory.containsKey(type.getTypeName())) {
        config.fieldMapping.put(entry.getKey(), type.getTypeName());
      }
    }
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    try {
      init(req);
    } catch (Exception e) {
      log.error(e);
    }
    return new MtasUpdateRequestProcessor(next, config);
  }
}

class MtasUpdateRequestProcessor extends UpdateRequestProcessor {
  private static Log log = LogFactory.getLog(MtasUpdateRequestProcessor.class);

  private MtasUpdateRequestProcessorConfig config;

  public MtasUpdateRequestProcessor(UpdateRequestProcessor next,
      MtasUpdateRequestProcessorConfig config) {
    super(next);
    this.config = config;
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    if (config != null && config.fieldMapping.size() > 0) {
      SolrInputDocument doc = cmd.getSolrInputDocument();
      for (String field : config.fieldMapping.keySet()) {
        processAdd(doc, field);
      }
    }
    super.processAdd(cmd);
  }

  private void processAdd(SolrInputDocument doc, String field) {
    SolrInputField originalValue = doc.get(field);
    String fieldType = config.fieldMapping.get(field);
    CharFilterFactory[] charFilterFactories = config.fieldTypeCharFilterFactories.get(fieldType);
    MtasTokenizerFactory tokenizerFactory = config.fieldTypeTokenizerFactory.get(config.fieldMapping.get(field));
    MtasUpdateRequestProcessorSizeReader sizeReader;
    if (originalValue != null
      && originalValue.getValue() instanceof String) {
      MtasUpdateRequestProcessorResultWriter result = null;
      try {
        String storedValue = (String) originalValue.getValue();
        Reader reader = new StringReader(storedValue);

        String configuration = null;
        String defaultConfiguration = config.fieldTypeDefaultConfiguration.get(fieldType);
        if (config.fieldTypeConfigurationFromField.get(fieldType) != null) {
          Object obj = doc.getFieldValue(config.fieldTypeConfigurationFromField.get(fieldType));
          if (obj != null) {
            configuration = obj.toString();
          }
        }

        if (charFilterFactories != null) {
          for (CharFilterFactory factory : charFilterFactories) {
            // We don't want the old pseudo-charfilters anymore. Remove this check once
            // the class MtasCharFilterFactory is dead.
            if (factory instanceof MtasCharFilterFactory) {
              continue;
            }
            reader = factory.create(reader);
          }
        }

        sizeReader = new MtasUpdateRequestProcessorSizeReader(reader);

        result = new MtasUpdateRequestProcessorResultWriter(storedValue);
        int numPositions = 0;
        Map<String, Integer> prefixes = new HashMap<>();
        String prefix;
        Integer prefixCount;
        try (MtasTokenizer tokenizer = tokenizerFactory.create(configuration, defaultConfiguration)) {
          tokenizer.setReader(sizeReader);
          tokenizer.reset();

          CharTermAttribute termAttribute = tokenizer
            .getAttribute(CharTermAttribute.class);
          OffsetAttribute offsetAttribute = tokenizer
            .getAttribute(OffsetAttribute.class);
          PositionIncrementAttribute positionIncrementAttribute = tokenizer
            .getAttribute(PositionIncrementAttribute.class);
          PayloadAttribute payloadAttribute = tokenizer
            .getAttribute(PayloadAttribute.class);
          FlagsAttribute flagsAttribute = tokenizer
            .getAttribute(FlagsAttribute.class);

          while (tokenizer.incrementToken()) {
            String term = null;
            Integer offsetStart = null;
            Integer offsetEnd = null;
            int posIncr = 0;
            Integer flags = null;
            BytesRef payload = null;
            if (termAttribute != null) {
              term = termAttribute.toString();
              prefix = CodecUtil.termPrefix(term);
              prefixCount = prefixes.get(prefix);
              if (prefixCount != null) {
                prefixes.put(prefix, prefixCount + 1);
              } else {
                prefixes.put(prefix, 1);
              }
            }
            if (offsetAttribute != null) {
              offsetStart = offsetAttribute.startOffset();
              offsetEnd = offsetAttribute.endOffset();
            }
            if (positionIncrementAttribute != null) {
              posIncr = positionIncrementAttribute.getPositionIncrement();
            }
            if (payloadAttribute != null) {
              payload = payloadAttribute.getPayload();
            }
            if (flagsAttribute != null) {
              flags = flagsAttribute.getFlags();
            }
            numPositions += posIncr;
            result.addItem(term, offsetStart, offsetEnd, posIncr, payload, flags);
          }

          // Store the temporary filename in the field, so that MtasPreAnalyzedParser
          // can pick it up later.
          doc.remove(field);
          if (result.getTokenNumber() > 0) {
            doc.addField(field, result.getFileName());
          }
        } finally {
          result.close();
        }

        setFields(doc, config.fieldTypeSizeField.get(fieldType), sizeReader.getTotalReadSize());
        setFields(doc, config.fieldTypeNumberOfPositionsField.get(fieldType), numPositions);
        setFields(doc, config.fieldTypeNumberOfTokensField.get(fieldType), result.getTokenNumber());
        setFields(doc, config.fieldTypePrefixField.get(fieldType), prefixes.keySet());
        if (config.fieldTypePrefixNumbersFieldPrefix.get(fieldType) != null) {
          for (Entry<String, Integer> prefixesEntry : prefixes.entrySet()) {
            setFields(doc, config.fieldTypePrefixNumbersFieldPrefix.get(fieldType) + prefixesEntry.getKey(),
              prefixesEntry.getValue());
          }
        }
      } catch (IOException e) {
        log.error(e);
        doc.addField(config.fieldTypeErrorField.get(fieldType), e.getMessage());
        setFields(doc, config.fieldTypeSizeField.get(fieldType), 0);
        setFields(doc, config.fieldTypeNumberOfPositionsField.get(fieldType), 0);
        setFields(doc, config.fieldTypeNumberOfTokensField.get(fieldType), 0);
        removeFields(doc, config.fieldTypePrefixField.get(fieldType));
        result.forceCloseAndDelete();
        doc.remove(field);
      }
    }
  }

  private void removeFields(SolrInputDocument doc, String fieldNames) {
    if (fieldNames != null) {
      for (String field : fieldNames.split(",")) {
        doc.removeField(field);
      }
    }
  }

  private void setFields(SolrInputDocument doc, String fieldNames, Object value) {
    if (fieldNames != null) {
      for (String field : fieldNames.split(",")) {
        field = field.trim();
        if (!field.isEmpty()) {
          doc.addField(field, value);
        }
      }
    }
  }
}

class MtasUpdateRequestProcessorConfig {
  HashMap<String, CharFilterFactory[]> fieldTypeCharFilterFactories = new HashMap<>();
  HashMap<String, MtasTokenizerFactory> fieldTypeTokenizerFactory = new HashMap<>();
  HashMap<String, String> fieldMapping = new HashMap<>();
  HashMap<String, String> fieldTypeDefaultConfiguration = new HashMap<>();
  HashMap<String, String> fieldTypeConfigurationFromField = new HashMap<>();
  HashMap<String, String> fieldTypeNumberOfTokensField = new HashMap<>();
  HashMap<String, String> fieldTypeNumberOfPositionsField = new HashMap<>();
  HashMap<String, String> fieldTypeSizeField = new HashMap<>();
  HashMap<String, String> fieldTypeErrorField = new HashMap<>();
  HashMap<String, String> fieldTypePrefixField = new HashMap<>();
  HashMap<String, String> fieldTypePrefixNumbersFieldPrefix = new HashMap<>();

  MtasUpdateRequestProcessorConfig() {
  }
}

class MtasUpdateRequestProcessorSizeReader extends Reader {
  Reader reader;
  long totalReadSize;

  public MtasUpdateRequestProcessorSizeReader(Reader reader) {
    this.reader = reader;
    totalReadSize = 0;
  }

  public int read(char[] cbuf, int off, int len) throws IOException {
    int read = reader.read(cbuf, off, len);
    totalReadSize += read;
    return read;
  }

  public void close() throws IOException {
    reader.close();
  }

  public long getTotalReadSize() {
    return totalReadSize;
  }
}
