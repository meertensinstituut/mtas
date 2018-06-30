package mtas.solr.handler;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

import mtas.analysis.MtasTokenizer;
import mtas.analysis.util.MtasFetchData;
import mtas.analysis.util.MtasParserException;
import mtas.solr.handler.component.MtasSolrSearchComponent;
import mtas.solr.handler.component.util.MtasSolrComponentStatus;
import mtas.solr.handler.util.MtasSolrHistoryList;
import mtas.solr.handler.util.MtasSolrRunningList;
import mtas.solr.handler.util.MtasSolrStatus;

// TODO: Auto-generated Javadoc
/**
 * The Class MtasRequestHandler.
 */
public class MtasRequestHandler extends RequestHandlerBase {

  /** The log. */
  private static Log log = LogFactory.getLog(MtasRequestHandler.class);

  /** The Constant MESSAGE_ERROR. */
  public static final String MESSAGE_ERROR = "error";

  /** The Constant ACTION_CONFIG_FILES. */
  public static final String ACTION_CONFIG_FILES = "files";

  /** The Constant ACTION_CONFIG_FILE. */
  public static final String ACTION_CONFIG_FILE = "file";

  /** The Constant ACTION_MAPPING. */
  public static final String ACTION_MAPPING = "mapping";

  /** The Constant ACTION_RUNNING. */
  public static final String ACTION_RUNNING = "running";

  /** The Constant ACTION_HISTORY. */
  public static final String ACTION_HISTORY = "history";

  /** The Constant ACTION_ERROR. */
  public static final String ACTION_ERROR = "error";

  /** The Constant ACTION_STATUS. */
  public static final String ACTION_STATUS = "status";

  /** The Constant PARAM_ACTION. */
  public static final String PARAM_ACTION = "action";

  /** The Constant PARAM_KEY. */
  public static final String PARAM_KEY = "key";

  /** The Constant PARAM_NUMBER. */
  public static final String PARAM_NUMBER = "key";

  /** The Constant PARAM_ABORT. */
  public static final String PARAM_ABORT = "abort";

  /** The Constant PARAM_SHARDREQUESTS. */
  public static final String PARAM_SHARDREQUESTS = "shardRequests";

  /** The Constant PARAM_CONFIG_FILE. */
  public static final String PARAM_CONFIG_FILE = "file";

  /** The Constant PARAM_MAPPING_CONFIGURATION. */
  public static final String PARAM_MAPPING_CONFIGURATION = "configuration";

  /** The Constant PARAM_MAPPING_DOCUMENT. */
  public static final String PARAM_MAPPING_DOCUMENT = "document";

  /** The Constant PARAM_MAPPING_DOCUMENT_URL. */
  public static final String PARAM_MAPPING_DOCUMENT_URL = "url";

  /** The running. */
  private MtasSolrRunningList running;

  /** The history. */
  private MtasSolrHistoryList history;

  /** The error. */
  private MtasSolrHistoryList error;

  /** The shard index. */
  private Map<String, ShardInformation> shardIndex;

  /** The status controller. */
  private StatusController statusController;

  /** The Constant defaultTimeout. */
  private static final int defaultTimeout = 3600;

  /** The Constant defaultSoftLimit. */
  private static final int defaultSoftLimit = 100;

  /** The Constant defaultHardLimit. */
  private static final int defaultHardLimit = 200;

  /** The Constant defaultNumber. */
  private static final int defaultNumber = 50;

  /**
   * Instantiates a new mtas request handler.
   */
  public MtasRequestHandler() {
    super();
    running = new MtasSolrRunningList(defaultTimeout);
    history = new MtasSolrHistoryList(defaultSoftLimit, defaultHardLimit);
    error = new MtasSolrHistoryList(defaultSoftLimit, defaultHardLimit);
    shardIndex = new HashMap<>();
    // start controller
    statusController = new StatusController();
    statusController.start();
  }

  /**
   * Handle request body.
   *
   * @param req the req
   * @param rsp the rsp
   * @throws IOException Signals that an I/O exception has occurred.
   */
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.handler.RequestHandlerBase#handleRequestBody(org.apache.
   * solr.request.SolrQueryRequest, org.apache.solr.response.SolrQueryResponse)
   */
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    String action;
    if ((action = req.getParams().get(PARAM_ACTION)) != null) {
      // generate list of files
      if (action.equals(ACTION_CONFIG_FILES)) {
        String configDir = req.getCore().getResourceLoader().getConfigDir();
        rsp.add(ACTION_CONFIG_FILES, getFiles(configDir, null));
        // get file
      } else if (action.equals(ACTION_CONFIG_FILE)) {
        String file = req.getParams().get(PARAM_CONFIG_FILE, null);
        if (file != null && !file.contains("..") && !file.startsWith("/")) {
          InputStream is;
          try {
            is = req.getCore().getResourceLoader().openResource(file);
            rsp.add(ACTION_CONFIG_FILE, IOUtils.toString(is, StandardCharsets.UTF_8));
          } catch (IOException e) {
            log.debug(e);
            rsp.add(MESSAGE_ERROR, e.getMessage());
          }
        }
        // test mapping
      } else if (action.equals(ACTION_MAPPING)) {
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
          InputStream stream = IOUtils.toInputStream(configuration, StandardCharsets.UTF_8);
          try (MtasTokenizer tokenizer = new MtasTokenizer(stream);) {
            MtasFetchData fetchData = new MtasFetchData(new StringReader(documentUrl));
            rsp.add(ACTION_MAPPING, tokenizer.getList(fetchData.getUrl(null, null)));
            tokenizer.close();
          } catch (IOException | MtasParserException e) {
            log.debug(e);
            rsp.add(MESSAGE_ERROR, e.getMessage());
          } finally {
            stream.close();
          }
        } else if (configuration != null && document != null) {
          InputStream stream = IOUtils.toInputStream(configuration, StandardCharsets.UTF_8);
          try (MtasTokenizer tokenizer = new MtasTokenizer(stream);) {
            rsp.add(ACTION_MAPPING, tokenizer.getList(new StringReader(document)));
            tokenizer.close();
          } catch (IOException e) {
            log.debug(e);
            rsp.add(MESSAGE_ERROR, e.getMessage());
          } finally {
            stream.close();
          }
        }
      } else if (action.equals(ACTION_RUNNING) || action.equals(ACTION_HISTORY) || action.equals(ACTION_ERROR)) {
        boolean shardRequests = req.getParams().getBool(PARAM_SHARDREQUESTS, false);
        int number;
        try {
          number = Integer.parseInt(req.getParams().get(PARAM_NUMBER));
        } catch (NumberFormatException e) {
          number = defaultNumber;
        }
        if (action.equals(ACTION_RUNNING)) {
          rsp.add(ACTION_RUNNING, running.createListOutput(shardRequests, number));
        } else if (action.equals(ACTION_HISTORY)) {
          rsp.add(ACTION_HISTORY, history.createListOutput(shardRequests, number));
        } else if (action.equals(ACTION_ERROR)) {
          rsp.add(ACTION_ERROR, error.createListOutput(shardRequests, number));
        }
      } else if (action.equals(ACTION_STATUS)) {
        String key = req.getParams().get(PARAM_KEY, null);
        String abort = req.getParams().get(PARAM_ABORT, null);
        MtasSolrStatus solrStatus = null;
        if ((solrStatus = history.get(key)) != null || (solrStatus = running.get(key)) != null
            || (solrStatus = error.get(key)) != null) {
          if (abort != null && !solrStatus.finished()) {
            solrStatus.setError(abort);
          }
          rsp.add(ACTION_STATUS, solrStatus.createItemOutput());
        } else {
          rsp.add(ACTION_STATUS, null);
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
    if (listOfFiles != null) {
      for (File file : listOfFiles) {
        String fullName = subDir == null ? file.getName() : subDir + File.separator + file.getName();
        if (file.isFile()) {
          files.add(fullName);
        } else if (file.isDirectory()) {
          files.addAll(getFiles(dir, fullName));
        }
      }
    }
    return files;
  }

  /**
   * Gets the description.
   *
   * @return the description
   */
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
   * Gets the params from JSON.
   *
   * @param params
   *          the params
   * @param json
   *          the json
   * @return the params from JSON
   */
  @SuppressWarnings("unchecked")
  private static void getParamsFromJSON(Map<String, String> params, String json) {
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
      log.debug("ignore parse exceptions at this stage, they may be caused by incomplete macro expansions", e);
      return;
    }
  }

  /**
   * Register status.
   *
   * @param item
   *          the item
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void registerStatus(MtasSolrStatus item) throws IOException {
    history.add(item);
    running.add(item);
  }

  /**
   * Finish status.
   *
   * @param item
   *          the item
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void finishStatus(MtasSolrStatus item) throws IOException {
    running.remove(item);
    if (item.error()) {
      error.add(item);
    }
  }

  /**
   * Check key.
   *
   * @param key
   *          the key
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void checkKey(String key) throws IOException {
    history.updateKey(key);
  }

  /**
   * Gets the shard information.
   *
   * @param shard
   *          the shard
   * @return the shard information
   */
  public ShardInformation getShardInformation(String shard) {
    ShardInformation shardInformation = shardIndex.get(Objects.requireNonNull(shard, "shard required"));
    if (shardInformation == null) {
      shardInformation = createShardInformation(shard);
    }
    return shardInformation;
  }

  /**
   * Creates the shard information.
   *
   * @param shard
   *          the shard
   * @return the shard information
   */
  private ShardInformation createShardInformation(String shard) {
    ShardInformation shardInformation = new ShardInformation(shard);
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.add(CommonParams.Q, "*:*");
    solrParams.add(CommonParams.ROWS, "0");
    solrParams.add(CommonParams.HEADER_ECHO_PARAMS, "none");
    solrParams.add(ShardParams.IS_SHARD, CommonParams.TRUE);
    solrParams.add(MtasSolrSearchComponent.PARAM_MTAS, CommonParams.TRUE);
    solrParams.add(MtasSolrComponentStatus.PARAM_MTAS_STATUS, CommonParams.TRUE);
    solrParams.add(
        MtasSolrComponentStatus.PARAM_MTAS_STATUS + "." + MtasSolrComponentStatus.NAME_MTAS_STATUS_MTASHANDLER,
        CommonParams.TRUE);
    solrParams.add(
        MtasSolrComponentStatus.PARAM_MTAS_STATUS + "." + MtasSolrComponentStatus.NAME_MTAS_STATUS_NUMBEROFSEGMENTS,
        CommonParams.TRUE);
    solrParams.add(
        MtasSolrComponentStatus.PARAM_MTAS_STATUS + "." + MtasSolrComponentStatus.NAME_MTAS_STATUS_NUMBEROFDOCUMENTS,
        CommonParams.TRUE);
    SolrClient solrClient = new HttpSolrClient.Builder(shard).build();
    try {  
      QueryResponse response = solrClient.query(solrParams);
      Object mtasHandlerObject = Objects.requireNonNull(
          response.getResponse().findRecursive(MtasSolrSearchComponent.NAME, MtasSolrComponentStatus.NAME,
              MtasSolrComponentStatus.NAME_MTAS_STATUS_MTASHANDLER),
          "no number of segments for " + shard);
      Object numberOfSegmentsObject = Objects.requireNonNull(
          response.getResponse().findRecursive(MtasSolrSearchComponent.NAME, MtasSolrComponentStatus.NAME,
              MtasSolrComponentStatus.NAME_MTAS_STATUS_NUMBEROFSEGMENTS),
          "no number of documents for " + shard);
      Object numberOfDocumentsObject = Objects.requireNonNull(
          response.getResponse().findRecursive(MtasSolrSearchComponent.NAME, MtasSolrComponentStatus.NAME,
              MtasSolrComponentStatus.NAME_MTAS_STATUS_NUMBEROFDOCUMENTS),
          "no name for " + shard);
      Object nameObject = Objects.requireNonNull(response.getResponse().findRecursive(MtasSolrSearchComponent.NAME,
          MtasSolrComponentStatus.NAME, ShardInformation.NAME_NAME), "no handler for " + shard);
      if (mtasHandlerObject instanceof String) {
        shardInformation.mtasHandler = (String) mtasHandlerObject;
      }
      if (numberOfSegmentsObject instanceof Integer) {
        shardInformation.numberOfSegments = (Integer) numberOfSegmentsObject;
      }
      if (numberOfDocumentsObject instanceof Integer) {
        shardInformation.numberOfDocuments = ((Integer) numberOfDocumentsObject).longValue();
      }
      if (nameObject instanceof String) {
        shardInformation.name = (String) nameObject;
      }
      shardIndex.put(shard, shardInformation);
    } catch (NullPointerException | SolrServerException | IOException e) {
      log.error(e);
      return null;
    } finally {
      if (solrClient != null) {
        try {
          solrClient.close();
        } catch (IOException e) {
          log.error(e);
        }
      }
    }
    return shardInformation;
  }

  /**
   * The Class StatusController.
   */
  public class StatusController extends Thread {

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
      List<MtasSolrStatus> statusWithException;
      while (true) {
        try {
          Thread.sleep(1000);
          statusWithException = running.checkForExceptions();
          if (statusWithException != null) {
            for (MtasSolrStatus item : statusWithException) {
              finishStatus(item);
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

    }

  }

  /**
   * The Class ShardInformation.
   */
  public static class ShardInformation {

    /** The Constant NAME_NAME. */
    public final static String NAME_NAME = "name";

    /** The number of documents. */
    public Long numberOfDocuments = null;

    /** The number of segments. */
    public Integer numberOfSegments = null;

    /** The mtas handler. */
    public String mtasHandler = null;

    /** The name. */
    public String name = null;

    /** The location. */
    private String location;

    /**
     * Instantiates a new shard information.
     *
     * @param location
     *          the location
     */
    public ShardInformation(String location) {
      this.location = location;
    }

    /**
     * Location.
     *
     * @return the string
     */
    public String location() {
      return location;
    }
  }

}
