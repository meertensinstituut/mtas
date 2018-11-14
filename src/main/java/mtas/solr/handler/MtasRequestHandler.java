package mtas.solr.handler;

import mtas.solr.handler.component.MtasSolrSearchComponent;
import mtas.solr.handler.component.util.MtasSolrComponentStatus;
import mtas.solr.handler.util.MtasSolrHistoryList;
import mtas.solr.handler.util.MtasSolrRunningList;
import mtas.solr.handler.util.MtasSolrStatus;
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
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Request handler that serves various bits of information about the mtas configuration.
 */
public class MtasRequestHandler extends RequestHandlerBase {
  private static Log log = LogFactory.getLog(MtasRequestHandler.class);

  public static final String MESSAGE_ERROR = "error";
  public static final String ACTION_CONFIG_FILES = "files";
  public static final String ACTION_CONFIG_FILE = "file";
  public static final String ACTION_RUNNING = "running";
  public static final String ACTION_HISTORY = "history";
  public static final String ACTION_ERROR = "error";
  public static final String ACTION_STATUS = "status";
  public static final String PARAM_ACTION = "action";
  public static final String PARAM_KEY = "key";
  public static final String PARAM_NUMBER = "key";
  public static final String PARAM_ABORT = "abort";
  public static final String PARAM_SHARDREQUESTS = "shardRequests";
  public static final String PARAM_CONFIG_FILE = "file";

  private MtasSolrRunningList running;
  private MtasSolrHistoryList history;
  private MtasSolrHistoryList error;
  private Map<String, ShardInformation> shardIndex;
  private StatusController statusController;

  private static final int defaultTimeout = 3600;
  private static final int defaultSoftLimit = 100;
  private static final int defaultHardLimit = 200;
  private static final int defaultNumber = 50;

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

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    String action = req.getParams().get(PARAM_ACTION);
    if (action == null) {
      return;
    }
    // generate list of files
    switch (action) {
      case ACTION_CONFIG_FILES:
        String configDir = req.getCore().getResourceLoader().getConfigDir();
        rsp.add(ACTION_CONFIG_FILES, getFiles(configDir, null));
        // get file
        break;
      case ACTION_CONFIG_FILE:
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
        break;
      case ACTION_RUNNING:
      case ACTION_HISTORY:
      case ACTION_ERROR:
        boolean shardRequests = req.getParams().getBool(PARAM_SHARDREQUESTS, false);
        int number;
        try {
          number = Integer.parseInt(req.getParams().get(PARAM_NUMBER));
        } catch (NumberFormatException e) {
          number = defaultNumber;
        }
        switch (action) {
          case ACTION_RUNNING:
            rsp.add(ACTION_RUNNING, running.createListOutput(shardRequests, number));
            break;
          case ACTION_HISTORY:
            rsp.add(ACTION_HISTORY, history.createListOutput(shardRequests, number));
            break;
          case ACTION_ERROR:
            rsp.add(ACTION_ERROR, error.createListOutput(shardRequests, number));
            break;
        }
        break;
      case ACTION_STATUS:
        String key = req.getParams().get(PARAM_KEY, null);
        String abort = req.getParams().get(PARAM_ABORT, null);
        MtasSolrStatus solrStatus;
        if ((solrStatus = history.get(key)) != null
          || (solrStatus = running.get(key)) != null
          || (solrStatus = error.get(key)) != null) {
          if (abort != null && !solrStatus.finished()) {
            solrStatus.setError(abort);
          }
          rsp.add(ACTION_STATUS, solrStatus.createItemOutput());
        } else {
          rsp.add(ACTION_STATUS, null);
        }
        break;
    }
  }

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

  @Override
  public String getDescription() {
    return "Mtas Request Handler";
  }

  public void registerStatus(MtasSolrStatus item) throws IOException {
    history.add(item);
    running.add(item);
  }

  public void finishStatus(MtasSolrStatus item) throws IOException {
    running.remove(item);
    if (item.error()) {
      error.add(item);
    }
  }

  public ShardInformation getShardInformation(String shard) {
    ShardInformation shardInformation = shardIndex.get(Objects.requireNonNull(shard, "shard required"));
    if (shardInformation == null) {
      shardInformation = createShardInformation(shard);
    }
    return shardInformation;
  }

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

  private class StatusController extends Thread {
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

  public static class ShardInformation {
    public final static String NAME_NAME = "name";
    public Long numberOfDocuments = null;
    public Integer numberOfSegments = null;
    public String mtasHandler = null;
    public String name = null;

    private String location;

    ShardInformation(String location) {
      this.location = location;
    }

    public String location() {
      return location;
    }
  }
}
