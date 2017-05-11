package mtas.solr.handler;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

import mtas.analysis.MtasTokenizer;
import mtas.analysis.util.MtasFetchData;
import mtas.analysis.util.MtasParserException;

/**
 * The Class MtasRequestHandler.
 */
public class MtasRequestHandler extends RequestHandlerBase {

  private static Log log = LogFactory.getLog(MtasRequestHandler.class);

  /** The error. */
  private final static String ERROR = "error";

  /** The action config files. */
  private final static String ACTION_CONFIG_FILES = "files";

  /** The action config file. */
  private final static String ACTION_CONFIG_FILE = "file";

  /** The action mapping. */
  private final static String ACTION_MAPPING = "mapping";

  /** The param action. */
  private final static String PARAM_ACTION = "action";

  /** The param config file. */
  private final static String PARAM_CONFIG_FILE = "file";

  /** The param mapping configuration. */
  private final static String PARAM_MAPPING_CONFIGURATION = "configuration";

  /** The param mapping document. */
  private final static String PARAM_MAPPING_DOCUMENT = "document";

  /** The param mapping document url. */
  private final static String PARAM_MAPPING_DOCUMENT_URL = "url";

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.solr.handler.RequestHandlerBase#handleRequestBody(org.apache.
   * solr.request.SolrQueryRequest, org.apache.solr.response.SolrQueryResponse)
   */
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
      throws IOException {

    String configDir = req.getCore().getResourceLoader().getConfigDir();
    // generate list of files
    if (req.getParams().get(PARAM_ACTION, "false")
        .equals(ACTION_CONFIG_FILES)) {
      rsp.add(ACTION_CONFIG_FILES, getFiles(configDir, null));
      // get file
    } else if (req.getParams().get(PARAM_ACTION, "false")
        .equals(ACTION_CONFIG_FILE)) {
      String file = req.getParams().get(PARAM_CONFIG_FILE, null);
      if (file != null && !file.contains("..")) {
        InputStream is;
        try {
          is = req.getCore().getResourceLoader().openResource(file);
          rsp.add(ACTION_CONFIG_FILE,
              IOUtils.toString(is, StandardCharsets.UTF_8));
        } catch (IOException e) {
          log.debug(e);
          rsp.add(ERROR, e.getMessage());
        }
      }
      // test mapping
    } else if (req.getParams().get(PARAM_ACTION, "false")
        .equals(ACTION_MAPPING)) {
      String configuration = null;
      String document = null;
      String documentUrl = null;
      if (req.getContentStreams() != null) {
        Iterator<ContentStream> it = req.getContentStreams().iterator();
        if (it.hasNext()) {
          ContentStream cs = it.next();
          Map<String, String> params = new HashMap<>();
          getParamsFromJSON(params, IOUtils.toString(cs.getReader()));
          configuration = params.get(PARAM_MAPPING_CONFIGURATION);
          document = params.get(PARAM_MAPPING_DOCUMENT);
          documentUrl = params.get(PARAM_MAPPING_DOCUMENT_URL);
        }
      } else {
        configuration = req.getParams().get(PARAM_MAPPING_CONFIGURATION);
        document = req.getParams().get(PARAM_MAPPING_DOCUMENT);
        documentUrl = req.getParams().get(PARAM_MAPPING_DOCUMENT_URL);
      }
      if (configuration != null && documentUrl != null) {
        InputStream stream = IOUtils.toInputStream(configuration,
            StandardCharsets.UTF_8);
        try (MtasTokenizer tokenizer = new MtasTokenizer(stream);) {
          MtasFetchData fetchData = new MtasFetchData(
              new StringReader(documentUrl));
          rsp.add(ACTION_MAPPING,
              tokenizer.getList(fetchData.getUrl(null, null)));
          tokenizer.close();
        } catch (IOException | MtasParserException e) {
          log.debug(e);
          rsp.add(ERROR, e.getMessage());
        } finally {
          stream.close();
        }
      } else if (configuration != null && document != null) {
        InputStream stream = IOUtils.toInputStream(configuration,
            StandardCharsets.UTF_8);
        try (MtasTokenizer tokenizer = new MtasTokenizer(stream);) {
          rsp.add(ACTION_MAPPING,
              tokenizer.getList(new StringReader(document)));
          tokenizer.close();
        } catch (IOException e) {
          log.debug(e);
          rsp.add(ERROR, e.getMessage());
        } finally {
          stream.close();
        }
      }
    }
  }

  /**
   * Gets the files.
   *
   * @param dir
   *          the dir
   * @param subDir
   *          the sub dir
   * @return the files
   */
  private ArrayList<String> getFiles(String dir, String subDir) {
    ArrayList<String> files = new ArrayList<>();
    String fullDir = subDir == null ? dir : dir + File.separator + subDir;
    File[] listOfFiles = (new File(fullDir)).listFiles();
    if(listOfFiles!=null) {
      for (File file : listOfFiles) {
        String fullName = subDir == null ? file.getName()
            : subDir + File.separator + file.getName();
        if (file.isFile()) {
          files.add(fullName);
        } else if (file.isDirectory()) {
          files.addAll(getFiles(dir, fullName));
        }
      }
    }  
    return files;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.handler.RequestHandlerBase#getDescription()
   */
  @Override
  public String getDescription() {
    return "Mtas Request Handler";
  }

  /**
   * Gets the params from json.
   *
   * @param params
   *          the params
   * @param json
   *          the json
   * @return the params from json
   */
  @SuppressWarnings("unchecked")
  private static void getParamsFromJSON(Map<String, String> params,
      String json) {
    JSONParser parser = new JSONParser(json);
    try {
      Object o = ObjectBuilder.getVal(parser);
      if (!(o instanceof Map))
        return;
      Map<String, Object> map = (Map<String, Object>) o;
      // To make consistent with json.param handling, we should make query
      // params come after json params (i.e. query params should
      // appear to overwrite json params.

      // Solr params are based on String though, so we need to convert
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        String key = entry.getKey();
        Object val = entry.getValue();
        if (params.get(key) != null) {
          continue;
        }

        if (val == null) {
          params.remove(key);
        } else if (val instanceof String) {
          params.put(key, (String) val);
        }
      }
    } catch (Exception e) {
      log.debug(
          "ignore parse exceptions at this stage, they may be caused by incomplete macro expansions",
          e);
      return;
    }
  }

}
