package mtas.solr.handler.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.response.SolrQueryResponse;
import mtas.codec.util.Status;
import mtas.solr.handler.MtasRequestHandler;
import mtas.solr.handler.component.util.MtasSolrComponentStatus;

// TODO: Auto-generated Javadoc
/**
 * The Class MtasSolrStatus.
 */
public class MtasSolrStatus {

  /** The Constant NAME_KEY. */
  private static final String NAME_KEY = "key";

  /** The Constant NAME_REQUEST. */
  private static final String NAME_REQUEST = "request";

  /** The Constant NAME_SHARDREQUEST. */
  private static final String NAME_SHARDREQUEST = "shardRequest";

  /** The Constant NAME_ERROR. */
  private static final String NAME_ERROR = "error";

  /** The Constant NAME_ABORT. */
  private static final String NAME_ABORT = "aborted";

  /** The Constant NAME_FINISHED. */
  private static final String NAME_FINISHED = "finished";

  /** The Constant NAME_TIME_TOTAL. */
  private static final String NAME_TIME_TOTAL = "timeTotal";

  /** The Constant NAME_TIME_START. */
  private static final String NAME_TIME_START = "timeStart";

  /** The Constant NAME_STATUS_NAME. */
  private static final String NAME_STATUS_NAME = "name";

  /** The Constant NAME_STATUS_TIME. */
  private static final String NAME_STATUS_TIME = "time";

  /** The Constant NAME_STATUS_ERROR. */
  private static final String NAME_STATUS_ERROR = "error";

  /** The Constant NAME_STATUS_ABORT. */
  private static final String NAME_STATUS_ABORT = "aborted";

  /** The Constant NAME_STATUS_FINISHED. */
  private static final String NAME_STATUS_FINISHED = "finished";

  /** The Constant NAME_STATUS_STAGES. */
  private static final String NAME_STATUS_STAGES = "stages";

  /** The Constant NAME_STATUS_STAGE. */
  private static final String NAME_STATUS_STAGE = "stage";

  /** The Constant NAME_STATUS_LAST. */
  private static final String NAME_STATUS_LAST = "last";

  /** The Constant NAME_STATUS_DISTRIBUTED. */
  private static final String NAME_STATUS_DISTRIBUTED = "distributed";

  /** The Constant NAME_STATUS_SHARDS. */
  private static final String NAME_STATUS_SHARDS = "shards";

  /** The Constant NAME_STATUS_SEGMENT_NUMBER_TOTAL. */
  private static final String NAME_STATUS_SEGMENT_NUMBER_TOTAL = "segmentNumberTotal";

  /** The Constant NAME_STATUS_SEGMENT_NUMBER_FINISHED. */
  private static final String NAME_STATUS_SEGMENT_NUMBER_FINISHED = "segmentNumberFinished";

  /** The Constant NAME_STATUS_SEGMENT_SUB_NUMBER_TOTAL. */
  private static final String NAME_STATUS_SEGMENT_SUB_NUMBER_TOTAL = "segmentSubNumberTotal";

  /** The Constant NAME_STATUS_SEGMENT_SUB_NUMBER_FINISHED. */
  private static final String NAME_STATUS_SEGMENT_SUB_NUMBER_FINISHED = "segmentSubNumberFinished";

  /** The Constant NAME_STATUS_SEGMENT_SUB_NUMBER_FINISHED_TOTAL. */
  private static final String NAME_STATUS_SEGMENT_SUB_NUMBER_FINISHED_TOTAL = "segmentSubNumberFinishedTotal";

  /** The Constant NAME_STATUS_DOCUMENT_NUMBER_TOTAL. */
  private static final String NAME_STATUS_DOCUMENT_NUMBER_TOTAL = "documentNumberTotal";

  /** The Constant NAME_STATUS_DOCUMENT_NUMBER_FOUND. */
  private static final String NAME_STATUS_DOCUMENT_NUMBER_FOUND = "documentNumberFound";

  /** The Constant NAME_STATUS_DOCUMENT_NUMBER_FINISHED. */
  private static final String NAME_STATUS_DOCUMENT_NUMBER_FINISHED = "documentNumberFinished";

  /** The Constant NAME_STATUS_DOCUMENT_SUB_NUMBER_TOTAL. */
  private static final String NAME_STATUS_DOCUMENT_SUB_NUMBER_TOTAL = "documentSubNumberTotal";

  /** The Constant NAME_STATUS_DOCUMENT_SUB_NUMBER_FINISHED. */
  private static final String NAME_STATUS_DOCUMENT_SUB_NUMBER_FINISHED = "documentSubNumberFinished";

  /** The Constant NAME_STATUS_DOCUMENT_SUB_NUMBER_FINISHED_TOTAL. */
  private static final String NAME_STATUS_DOCUMENT_SUB_NUMBER_FINISHED_TOTAL = "documentSubNumberFinishedTotal";

  /** The key. */
  private String key;

  /** The current stage. */
  private Integer currentStage = null;

  /** The shard key. */
  private String shardKey = null;

  /** The shard stage keys. */
  private Map<Integer, String> shardStageKeys;

  /** The shard stage status. */
  private Map<Integer, StageStatus> shardStageStatus;

  /** The status. */
  private volatile Status status;

  /** The request. */
  private volatile String request;

  /** The shard request. */
  private volatile boolean shardRequest;

  /** The error status. */
  private volatile boolean errorStatus;

  /** The error message. */
  private volatile String errorMessage;

  /** The abort status. */
  private volatile boolean abortStatus;

  /** The abort message. */
  private volatile String abortMessage;

  /** The start time. */
  private volatile long startTime;

  /** The total time. */
  private volatile Integer totalTime;

  /** The finished. */
  private volatile boolean finished;

  /** The shards. */
  private volatile Map<String, ShardStatus> shards;

  /** The shard number total. */
  private volatile int shardNumberTotal;

  /** The shard info update. */
  private volatile Long shardInfoUpdate;

  /** The shard info updated. */
  private volatile boolean shardInfoUpdated;

  /** The shard info need update. */
  private volatile boolean shardInfoNeedUpdate;

  /** The shard info error. */
  private volatile boolean shardInfoError;

  /** The rsp. */
  private SolrQueryResponse rsp;

  /**
   * Instantiates a new mtas solr status.
   *
   * @param request
   *          the request
   * @param shardRequest
   *          the shard request
   * @param shards
   *          the shards
   * @param rsp
   *          the rsp
   */
  public MtasSolrStatus(String request, boolean shardRequest, String[] shards, SolrQueryResponse rsp) {
    key = null;
    status = new Status();
    this.request = request;
    this.shardRequest = shardRequest;
    abortMessage = null;
    abortStatus = false;
    errorMessage = null;
    errorStatus = false;
    startTime = System.currentTimeMillis();
    totalTime = null;
    finished = false;
    if (shards != null && shards.length > 0) {
      this.shards = new ConcurrentHashMap<>();
      this.shardStageKeys = new ConcurrentHashMap<>();
      this.shardStageStatus = new ConcurrentHashMap<>();
      for (String shard : shards) {
        if (shard == null) {
          shardNumberTotal = 0;
          this.shards = null;
          this.shardStageKeys = null;
          this.shardStageStatus = null;
          break;
        }
        this.shards.put(shard, new ShardStatus());
      }
    } else {
      shardNumberTotal = 0;
      this.shards = null;
      this.shardStageKeys = null;
      this.shardStageStatus = null;
    }
    shardNumberTotal = shards == null ? 0 : shards.length;
    shardInfoUpdate = null;
    shardInfoUpdated = false;
    shardInfoNeedUpdate = true;
    shardInfoError = false;
    this.rsp = rsp;
  }

  /**
   * Key.
   *
   * @return the string
   */
  public final String key() {
    key = (key == null) ? UUID.randomUUID().toString() : key;
    return key;
  }

  /**
   * Shard key.
   *
   * @param stage
   *          the stage
   * @return the string
   */
  public final String shardKey(int stage) {
    if (shardStageKeys == null) {
      return null;
    } else {
      shardKey = shardStageKeys.get(stage);
      if (shardKey == null) {
        shardKey = UUID.randomUUID().toString();
        shardStageKeys.put(stage, shardKey);
      }
      return shardKey;
    }
  }

  /**
   * Sets the stage.
   *
   * @param stage
   *          the new stage
   */
  public final void setStage(int stage) {
    this.currentStage = stage;
  }

  /**
   * Sets the abort.
   *
   * @param message
   *          the new abort
   */
  public final void setAbort(String message) {
    this.abortMessage = Objects.requireNonNull(message, "message required");
    this.abortStatus = true;
  }

  /**
   * Sets the error.
   *
   * @param exception
   *          the new error
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public final void setError(IOException exception) throws IOException {
    Objects.requireNonNull(exception, "exception required");
    errorMessage = exception.getMessage();
    // StringWriter sw = new StringWriter();
    // exception.printStackTrace(new PrintWriter(sw));
    // errorMessage+="\n====\n"+sw.toString();
    errorStatus = true;
    throw exception;
  }

  /**
   * Sets the error.
   *
   * @param error
   *          the new error
   */
  public final void setError(String error) {
    this.errorMessage = Objects.requireNonNull(error, "error required");
    this.errorStatus = true;
  }

  /**
   * Sets the finished.
   */
  public final void setFinished() {
    if (!finished) {
      totalTime = ((Long) (System.currentTimeMillis() - startTime)).intValue();
      shardInfoUpdated = false;
      finished = true;
      rsp = null;
    }
  }

  /**
   * Sets the key.
   *
   * @param key
   *          the new key
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void setKey(String key) throws IOException {
    if (this.key != null) {
      throw new IOException("key already set");
    } else {
      this.key = Objects.requireNonNull(key, "key required");
    }
  }

  /**
   * Shard request.
   *
   * @return true, if successful
   */
  public final boolean shardRequest() {
    return shardRequest;
  }

  /**
   * Gets the shards.
   *
   * @return the shards
   */
  public final Map<String, ShardStatus> getShards() {
    return shards;
  }

  /**
   * Status.
   *
   * @return the status
   */
  public final Status status() {
    return status;
  }

  /**
   * Error.
   *
   * @return true, if successful
   */
  public final boolean error() {
    return errorStatus;
  }

  /**
   * Abort.
   *
   * @return true, if successful
   */
  public final boolean abort() {
    return abortStatus;
  }

  /**
   * Finished.
   *
   * @return true, if successful
   */
  public final boolean finished() {
    return finished;
  }

  /**
   * Abort message.
   *
   * @return the string
   */
  public final String abortMessage() {
    return Objects.requireNonNull(abortMessage, "no abortMessage available");
  }

  /**
   * Error message.
   *
   * @return the string
   */
  public final String errorMessage() {
    return Objects.requireNonNull(errorMessage, "no errorMessage available");
  }

  /**
   * Gets the start time.
   *
   * @return the start time
   */
  public final Long getStartTime() {
    return startTime;
  }

  /**
   * Check response on exception.
   *
   * @return true, if successful
   */
  public boolean checkResponseForException() {
    if (!finished && rsp != null) {
      Exception e;
      if ((e = rsp.getException()) != null) {
        setError(e.getMessage());
        setFinished();
        return true;
      }
    }
    return false;
  }

  /**
   * Creates the list output.
   *
   * @return the simple ordered map
   */
  public SimpleOrderedMap<Object> createListOutput() {
    return createOutput(false);
  }

  /**
   * Creates the item output.
   *
   * @return the simple ordered map
   */
  public SimpleOrderedMap<Object> createItemOutput() {
    return createOutput(true);
  }

  /**
   * Creates the output.
   *
   * @param detailed the detailed
   * @return the simple ordered map
   */
  private SimpleOrderedMap<Object> createOutput(boolean detailed) {
    // checkResponseOnException();
    SimpleOrderedMap<Object> output = new SimpleOrderedMap<>();
    updateShardInfo();
    output.add(NAME_KEY, key);
    output.add(NAME_REQUEST, request);
    if (errorStatus) {
      output.add(NAME_ERROR, errorMessage);
    }
    if (abortStatus) {
      output.add(NAME_ABORT, abortMessage);
    }
    output.add(NAME_FINISHED, finished);
    if (totalTime != null) {
      output.add(NAME_TIME_TOTAL, totalTime);
    }
    output.add(NAME_TIME_START, (new Date(startTime)).toString());
    output.add(NAME_SHARDREQUEST, shardRequest);
    if (shardNumberTotal > 0) {
      if (detailed) {
        output.add(NAME_STATUS_DISTRIBUTED, createShardsOutput());
      } else {
        output.add(NAME_STATUS_DISTRIBUTED, true);
      }
    } else if (detailed) {
      output.add(NAME_STATUS_DISTRIBUTED, false);
    }
    if (status.numberSegmentsTotal != null) {
      output.add(NAME_STATUS_SEGMENT_NUMBER_TOTAL, status.numberSegmentsTotal);
      output.add(NAME_STATUS_SEGMENT_NUMBER_FINISHED, status.numberSegmentsFinished);
      if (!status.subNumberSegmentsFinished.isEmpty()) {
        output.add(NAME_STATUS_SEGMENT_SUB_NUMBER_FINISHED, status.subNumberSegmentsFinished);
        output.add(NAME_STATUS_SEGMENT_SUB_NUMBER_TOTAL, status.subNumberSegmentsTotal);
        output.add(NAME_STATUS_SEGMENT_SUB_NUMBER_FINISHED_TOTAL, status.subNumberSegmentsFinishedTotal);
      }
    }
    if (status.numberDocumentsTotal != null) {
      output.add(NAME_STATUS_DOCUMENT_NUMBER_TOTAL, status.numberDocumentsTotal);
      output.add(NAME_STATUS_DOCUMENT_NUMBER_FOUND, status.numberDocumentsFound);
      output.add(NAME_STATUS_DOCUMENT_NUMBER_FINISHED, status.numberDocumentsFinished);
      if (!status.subNumberDocumentsFinished.isEmpty()) {
        output.add(NAME_STATUS_DOCUMENT_SUB_NUMBER_FINISHED, status.subNumberDocumentsFinished);
        output.add(NAME_STATUS_DOCUMENT_SUB_NUMBER_TOTAL, status.subNumberDocumentsTotal);
        output.add(NAME_STATUS_DOCUMENT_SUB_NUMBER_FINISHED_TOTAL, status.subNumberDocumentsFinishedTotal);
      }
    }
    return output;
  }

  /**
   * Creates the shards output.
   *
   * @return the simple ordered map
   */
  private final SimpleOrderedMap<Object> createShardsOutput() {
    SimpleOrderedMap<Object> output = new SimpleOrderedMap<>();
    if (shardStageStatus != null && !shardStageStatus.isEmpty()) {
      List<SimpleOrderedMap<Object>> list = new ArrayList<>();
      for (StageStatus stageStatus : shardStageStatus.values()) {
        list.add(stageStatus.createOutput());
      }
      output.add(NAME_STATUS_STAGES, list);
    }
    if (shards != null && !shards.isEmpty()) {
      List<SimpleOrderedMap<Object>> list = new ArrayList<>();
      for (ShardStatus shardStatus : shards.values()) {
        list.add(shardStatus.createOutput());
      }
      output.add(NAME_STATUS_SHARDS, list);
    }
    return output;
  }

  /**
   * Gets the integer.
   *
   * @param response
   *          the response
   * @param args
   *          the args
   * @return the integer
   */
  private final Integer getInteger(NamedList<Object> response, String... args) {
    Object objectItem = response.findRecursive(args);
    if (objectItem != null && objectItem instanceof Integer) {
      return (Integer) objectItem;
    } else {
      return null;
    }
  }

  /**
   * Gets the integer map.
   *
   * @param response
   *          the response
   * @param args
   *          the args
   * @return the integer map
   */
  private final Map<String, Integer> getIntegerMap(NamedList<Object> response, String... args) {
    Object objectItem = response.findRecursive(args);
    Map<String, Integer> result = null;
    if (objectItem != null && objectItem instanceof Map) {
      result = (Map) objectItem;
    }
    return result;
  }

  /**
   * Gets the long.
   *
   * @param response
   *          the response
   * @param args
   *          the args
   * @return the long
   */
  private final Long getLong(NamedList<Object> response, String... args) {
    Object objectItem = response.findRecursive(args);
    if (objectItem != null && objectItem instanceof Long) {
      return (Long) objectItem;
    } else {
      return null;
    }
  }

  /**
   * Gets the long map.
   *
   * @param response
   *          the response
   * @param args
   *          the args
   * @return the long map
   */
  private final Map<String, Long> getLongMap(NamedList<Object> response, String... args) {
    Object objectItem = response.findRecursive(args);
    if (objectItem != null && objectItem instanceof Map) {
      return (Map) objectItem;
    } else {
      return null;
    }
  }

  /**
   * Gets the string.
   *
   * @param response
   *          the response
   * @param args
   *          the args
   * @return the string
   */
  private final String getString(NamedList<Object> response, String... args) {
    Object objectItem = response.findRecursive(args);
    if (objectItem != null && objectItem instanceof String) {
      return (String) objectItem;
    } else {
      return null;
    }
  }

  /**
   * Gets the boolean.
   *
   * @param response
   *          the response
   * @param args
   *          the args
   * @return the boolean
   */
  private final Boolean getBoolean(NamedList<Object> response, String... args) {
    Object objectItem = response.findRecursive(args);
    Boolean result = null;
    if (objectItem != null && objectItem instanceof Boolean) {
      result = (Boolean) objectItem;
    }
    return result;
  }

  /**
   * Update shard info.
   */
  private final void updateShardInfo() {
    final long expirationTime = 1000;
    // don't update too much
    if (shardKey == null || (shardInfoUpdated
        && Objects.requireNonNull(shardInfoUpdate, "update expire time not set") < System.currentTimeMillis())) {
      return;
    }
    // and only if necessary
    if (!shardInfoUpdated || !finished || shardInfoNeedUpdate) {
      // reset
      shardInfoError = false;
      // get list of relevant stages
      Set<Integer> stagesList = new HashSet<>();
      for (Integer stage : shardStageKeys.keySet()) {
        if (!shardStageStatus.containsKey(stage) || !shardStageStatus.get(stage).finished) {
          stagesList.add(stage);
        }
      }
      // loop for this list over shards
      for (Entry<String, ShardStatus> entry : shards.entrySet()) {
        ShardStatus shardStatus = entry.getValue();
        SolrClient solrClient = null;
        StageStatus stageStatus;
        // then loop over stages
        for (Integer stage : stagesList) {
          // get shardStage
          if (!shardStageStatus.containsKey(stage)) {
            stageStatus = new StageStatus(stage);
            shardStageStatus.put(stage, stageStatus);
          } else {
            stageStatus = shardStageStatus.get(stage);
          }
          if (shardStatus.finishedStages.contains(stage)) {
            stageStatus.add(true, shardStatus.numberDocumentsFoundStage.get(stage));
          } else {
            // create request
            ModifiableSolrParams solrParams = new ModifiableSolrParams();
            solrParams.add(CommonParams.QT, shardStatus.mtasHandler);
            solrParams.add(MtasRequestHandler.PARAM_ACTION, MtasRequestHandler.ACTION_STATUS);
            solrParams.add(MtasRequestHandler.PARAM_KEY, shardStageKeys.get(stage));

            try {
              // set solrClient
              solrClient = new HttpSolrClient.Builder(shardStatus.location).build();
              // get response
              QueryResponse response = solrClient.query(solrParams);
              // check for response
              if (response.getResponse().findRecursive(MtasSolrComponentStatus.NAME) != null) {
                shardStatus.numberDocumentsFoundStage.put(stage,
                    getLong(response.getResponse(), MtasSolrComponentStatus.NAME, NAME_STATUS_DOCUMENT_NUMBER_FOUND));
                shardStatus.timeStage.put(stage,
                    getInteger(response.getResponse(), MtasSolrComponentStatus.NAME, NAME_TIME_TOTAL));
                stageStatus.add(
                    shardStatus.setFinishedStage(stage,
                        getBoolean(response.getResponse(), MtasSolrComponentStatus.NAME, NAME_FINISHED)),
                    shardStatus.numberDocumentsFoundStage.get(stage));
                shardInfoError = shardStatus.setErrorStage(stage,
                    getString(response.getResponse(), MtasSolrComponentStatus.NAME, NAME_ERROR)) || shardInfoError;
                shardStatus.setAbortStage(stage,
                    getString(response.getResponse(), MtasSolrComponentStatus.NAME, NAME_ABORT));
                if (stage.equals(currentStage)) {
                  shardStatus.stage = stage;
                  shardStatus.stageNumberDocumentsFinished = getLong(response.getResponse(),
                      MtasSolrComponentStatus.NAME, NAME_STATUS_DOCUMENT_NUMBER_FINISHED);
                  shardStatus.stageSubNumberDocumentsFinished = getLongMap(response.getResponse(),
                      MtasSolrComponentStatus.NAME, NAME_STATUS_DOCUMENT_SUB_NUMBER_FINISHED);
                  shardStatus.stageSubNumberDocumentsTotal = getLong(response.getResponse(),
                      MtasSolrComponentStatus.NAME, NAME_STATUS_DOCUMENT_SUB_NUMBER_TOTAL);
                  shardStatus.stageSubNumberDocumentsFinishedTotal = getLong(response.getResponse(),
                      MtasSolrComponentStatus.NAME, NAME_STATUS_DOCUMENT_SUB_NUMBER_FINISHED_TOTAL);
                  shardStatus.stageNumberSegmentsFinished = getInteger(response.getResponse(),
                      MtasSolrComponentStatus.NAME, NAME_STATUS_SEGMENT_SUB_NUMBER_FINISHED_TOTAL);
                  shardStatus.stageSubNumberSegmentsFinished = getIntegerMap(response.getResponse(),
                      MtasSolrComponentStatus.NAME, NAME_STATUS_SEGMENT_SUB_NUMBER_FINISHED);
                  shardStatus.stageSubNumberSegmentsTotal = getInteger(response.getResponse(),
                      MtasSolrComponentStatus.NAME, NAME_STATUS_SEGMENT_SUB_NUMBER_TOTAL);
                  shardStatus.stageSubNumberSegmentsFinishedTotal = getInteger(response.getResponse(),
                      MtasSolrComponentStatus.NAME, NAME_STATUS_SEGMENT_SUB_NUMBER_FINISHED_TOTAL);
                }
              } else if (!finished && currentStage.equals(stage) && !stageStatus.checked) {
                stageStatus.finished = false;
              }
            } catch (SolrServerException | IOException e) {
              shardInfoError = shardInfoError || shardStatus.setErrorStage(stage, e.getMessage());
            } finally {
              if (solrClient != null) {
                try {
                  solrClient.close();
                } catch (IOException e) {
                  shardInfoError = shardInfoError || shardStatus.setErrorStage(stage, e.getMessage());
                }
              }
            }
          }
        }
      }
      if (!shardInfoError) {
        for (StageStatus item : shardStageStatus.values()) {
          item.checked = true;
        }
        shardInfoUpdated = true;
        shardInfoUpdate = System.currentTimeMillis() + expirationTime;
      } else {
        shardInfoUpdated = false;
        shardInfoUpdate = null;
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return createItemOutput().toString();
  }

  /**
   * The Class StageStatus.
   */
  public static class StageStatus {

    /** The stage. */
    private int stage;

    /** The checked. */
    public boolean checked;

    /** The finished. */
    public boolean finished;

    /** The number of documents found. */
    public int numberOfDocumentsFound;

    /** The number of shards. */
    public int numberOfShards;

    /**
     * Instantiates a new stage status.
     *
     * @param stage
     *          the stage
     */
    public StageStatus(int stage) {
      this.stage = stage;
      reset();
    }

    /**
     * Reset.
     */
    public void reset() {
      finished = true;
      checked = false;
      numberOfDocumentsFound = 0;
      numberOfShards = 0;
    }

    /**
     * Adds the.
     *
     * @param finished the finished
     * @param numberOfDocumentsFound the number of documents found
     */
    public void add(boolean finished, Long numberOfDocumentsFound) {
      if (!finished) {
        this.finished = false;
      }
      if (numberOfDocumentsFound != null) {
        this.numberOfDocumentsFound += numberOfDocumentsFound;
      }
      numberOfShards++;
    }

    /**
     * Creates the output.
     *
     * @return the simple ordered map
     */
    public SimpleOrderedMap<Object> createOutput() {
      SimpleOrderedMap<Object> stageOutput = new SimpleOrderedMap<>();
      stageOutput.add(NAME_FINISHED, finished);
      stageOutput.add(NAME_STATUS_DOCUMENT_NUMBER_FOUND, numberOfDocumentsFound);
      return stageOutput;
    }

    /**
     * Stage.
     *
     * @return the int
     */
    public int stage() {
      return stage;
    }
  }

  /**
   * The Class ShardStatus.
   */
  public class ShardStatus {

    /** The name. */
    public String name;

    /** The location. */
    public String location;

    /** The mtas handler. */
    public String mtasHandler;

    /** The number segments total. */
    public Integer numberSegmentsTotal = null;

    /** The number documents total. */
    public Long numberDocumentsTotal = null;

    /** The number documents found stage. */
    public Map<Integer, Long> numberDocumentsFoundStage = new HashMap<>();

    /** The error stage. */
    public Map<Integer, String> errorStage = null;

    /** The abort stage. */
    public Map<Integer, String> abortStage = null;

    /** The stage. */
    public Integer stage = null;

    /** The time stage. */
    public Map<Integer, Integer> timeStage = new HashMap<>();

    /** The finished stages. */
    public Set<Integer> finishedStages = new HashSet<>();

    /** The stage number segments finished. */
    public Integer stageNumberSegmentsFinished = null;

    /** The stage sub number segments total. */
    public Integer stageSubNumberSegmentsTotal = null;

    /** The stage sub number segments finished total. */
    public Integer stageSubNumberSegmentsFinishedTotal = null;

    /** The stage sub number segments finished. */
    public Map<String, Integer> stageSubNumberSegmentsFinished = new HashMap<>();

    /** The stage number documents finished. */
    public Long stageNumberDocumentsFinished = null;

    /** The stage sub number documents total. */
    public Long stageSubNumberDocumentsTotal = null;

    /** The stage sub number documents finished total. */
    public Long stageSubNumberDocumentsFinishedTotal = null;

    /** The stage sub number documents finished. */
    public Map<String, Long> stageSubNumberDocumentsFinished = new HashMap<>();

    /**
     * Creates the output.
     *
     * @return the simple ordered map
     */
    public SimpleOrderedMap<Object> createOutput() {
      SimpleOrderedMap<Object> shardOutput = new SimpleOrderedMap<>();
      if (name != null) {
        shardOutput.add(NAME_STATUS_NAME, name);
      }
      if (numberSegmentsTotal != null) {
        shardOutput.add(NAME_STATUS_SEGMENT_NUMBER_TOTAL, numberSegmentsTotal);
      }
      if (numberDocumentsTotal != null) {
        shardOutput.add(NAME_STATUS_DOCUMENT_NUMBER_TOTAL, numberDocumentsTotal);
      }
      // stages
      Map<Integer, SimpleOrderedMap<Object>> shardStagesOutput = new HashMap<>();
      for (Integer s : shardStageKeys.keySet()) {
        if (numberDocumentsFoundStage.containsKey(s)) {
          SimpleOrderedMap<Object> shardStageOutput = new SimpleOrderedMap<>();
          shardStageOutput.add(NAME_STATUS_DOCUMENT_NUMBER_FOUND, numberDocumentsFoundStage.get(s));
          shardStageOutput.add(NAME_STATUS_TIME, timeStage.get(s));
          shardStageOutput.add(NAME_STATUS_FINISHED, finishedStages.contains(s));
          if (errorStage != null && errorStage.containsKey(s)) {
            shardStageOutput.add(NAME_STATUS_ERROR, errorStage.get(s));
          }
          if (abortStage != null && abortStage.containsKey(s)) {
            shardStageOutput.add(NAME_STATUS_ABORT, abortStage.get(s));
          }
          shardStagesOutput.put(s, shardStageOutput);
        }
      }
      if (!shardStagesOutput.isEmpty()) {
        shardOutput.add(NAME_STATUS_STAGES, shardStagesOutput);
      }
      // (last) active stage
      if (stage != null) {
        SimpleOrderedMap<Object> shardStageOutput = new SimpleOrderedMap<>();
        shardStageOutput.add(NAME_STATUS_STAGE, stage);
        if (stageNumberSegmentsFinished != null) {
          shardOutput.add(NAME_STATUS_SEGMENT_NUMBER_FINISHED, stageNumberSegmentsFinished);
        }
        if (stageSubNumberSegmentsFinishedTotal != null) {
          shardOutput.add(NAME_STATUS_SEGMENT_SUB_NUMBER_FINISHED_TOTAL, stageSubNumberSegmentsFinishedTotal);
        }
        if (stageSubNumberSegmentsTotal != null) {
          shardOutput.add(NAME_STATUS_SEGMENT_SUB_NUMBER_TOTAL, stageSubNumberSegmentsTotal);
        }
        if (stageSubNumberSegmentsFinished != null && !stageSubNumberSegmentsFinished.isEmpty()) {
          shardOutput.add(NAME_STATUS_SEGMENT_SUB_NUMBER_FINISHED, stageSubNumberSegmentsFinished);
        }
        if (stageNumberDocumentsFinished != null) {
          shardOutput.add(NAME_STATUS_DOCUMENT_NUMBER_FINISHED, stageNumberDocumentsFinished);
        }
        if (stageSubNumberDocumentsTotal != null) {
          shardOutput.add(NAME_STATUS_DOCUMENT_SUB_NUMBER_TOTAL, stageSubNumberDocumentsTotal);
        }
        if (stageSubNumberDocumentsFinishedTotal != null) {
          shardOutput.add(NAME_STATUS_DOCUMENT_SUB_NUMBER_FINISHED_TOTAL, stageSubNumberDocumentsFinishedTotal);
        }
        if (stageSubNumberDocumentsFinished != null && !stageSubNumberDocumentsFinished.isEmpty()) {
          shardOutput.add(NAME_STATUS_DOCUMENT_SUB_NUMBER_FINISHED, stageSubNumberDocumentsFinished);
        }
        shardOutput.add(NAME_STATUS_LAST, shardStageOutput);
      }
      return shardOutput;
    }

    /**
     * Sets the finished stage.
     *
     * @param stage
     *          the stage
     * @param finished
     *          the finished
     * @return true, if successful
     */
    public boolean setFinishedStage(Integer stage, Boolean finished) {
      if (finished != null) {
        if (finished) {
          finishedStages.add(stage);
          return true;
        } else {
          finishedStages.remove(stage);
        }
      }
      return false;
    }

    /**
     * Sets the error stage.
     *
     * @param stage
     *          the stage
     * @param error
     *          the error
     * @return true, if successful
     */
    public boolean setErrorStage(Integer stage, String error) {
      if (error != null) {
        if (errorStage == null) {
          errorStage = new HashMap<>();
        }
        errorStage.put(stage, error);
        return true;
      } else {
        return false;
      }
    }

    /**
     * Sets the abort stage.
     *
     * @param stage
     *          the stage
     * @param abort
     *          the abort
     * @return true, if successful
     */
    public boolean setAbortStage(Integer stage, String abort) {
      if (abort != null) {
        if (abortStage == null) {
          abortStage = new HashMap<>();
        }
        abortStage.put(stage, abort);
        return true;
      } else {
        return false;
      }
    }

  }

}
