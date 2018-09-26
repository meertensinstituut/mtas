package mtas.solr.handler.util;

import mtas.codec.util.Status;
import mtas.solr.handler.MtasRequestHandler;
import mtas.solr.handler.component.util.MtasSolrComponentStatus;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.response.SolrQueryResponse;

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

public class MtasSolrStatus {
  private static final String NAME_KEY = "key";
  private static final String NAME_REQUEST = "request";
  private static final String NAME_SHARDREQUEST = "shardRequest";
  private static final String NAME_ERROR = "error";
  private static final String NAME_ABORT = "aborted";
  private static final String NAME_FINISHED = "finished";
  private static final String NAME_TIME_TOTAL = "timeTotal";
  private static final String NAME_TIME_START = "timeStart";
  private static final String NAME_STATUS_NAME = "name";
  private static final String NAME_STATUS_TIME = "time";
  private static final String NAME_STATUS_ERROR = "error";
  private static final String NAME_STATUS_ABORT = "aborted";
  private static final String NAME_STATUS_FINISHED = "finished";
  private static final String NAME_STATUS_STAGES = "stages";
  private static final String NAME_STATUS_STAGE = "stage";
  private static final String NAME_STATUS_LAST = "last";
  private static final String NAME_STATUS_DISTRIBUTED = "distributed";
  private static final String NAME_STATUS_SHARDS = "shards";
  private static final String NAME_STATUS_SEGMENT_NUMBER_TOTAL = "segmentNumberTotal";
  private static final String NAME_STATUS_SEGMENT_NUMBER_FINISHED = "segmentNumberFinished";
  private static final String NAME_STATUS_SEGMENT_SUB_NUMBER_TOTAL = "segmentSubNumberTotal";
  private static final String NAME_STATUS_SEGMENT_SUB_NUMBER_FINISHED = "segmentSubNumberFinished";
  private static final String NAME_STATUS_SEGMENT_SUB_NUMBER_FINISHED_TOTAL = "segmentSubNumberFinishedTotal";
  private static final String NAME_STATUS_DOCUMENT_NUMBER_TOTAL = "documentNumberTotal";
  private static final String NAME_STATUS_DOCUMENT_NUMBER_FOUND = "documentNumberFound";
  private static final String NAME_STATUS_DOCUMENT_NUMBER_FINISHED = "documentNumberFinished";
  private static final String NAME_STATUS_DOCUMENT_SUB_NUMBER_TOTAL = "documentSubNumberTotal";
  private static final String NAME_STATUS_DOCUMENT_SUB_NUMBER_FINISHED = "documentSubNumberFinished";
  private static final String NAME_STATUS_DOCUMENT_SUB_NUMBER_FINISHED_TOTAL = "documentSubNumberFinishedTotal";

  private String key;
  private Integer currentStage = null;
  private String shardKey = null;
  private Map<Integer, String> shardStageKeys;
  private Map<Integer, StageStatus> shardStageStatus;

  private volatile Status status;
  private volatile String request;
  private volatile boolean shardRequest;
  private volatile boolean errorStatus;
  private volatile String errorMessage;
  private volatile boolean abortStatus;
  private volatile String abortMessage;
  private volatile long startTime;
  private volatile Integer totalTime;
  private volatile boolean finished;
  private volatile Map<String, ShardStatus> shards;
  private volatile int shardNumberTotal;
  private volatile Long shardInfoUpdate;
  private volatile boolean shardInfoUpdated;
  private volatile boolean shardInfoNeedUpdate;
  private volatile boolean shardInfoError;

  private SolrQueryResponse rsp;

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

  public final String key() {
    key = (key == null) ? UUID.randomUUID().toString() : key;
    return key;
  }

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

  public final void setStage(int stage) {
    this.currentStage = stage;
  }

  public final void setAbort(String message) {
    this.abortMessage = Objects.requireNonNull(message, "message required");
    this.abortStatus = true;
  }

  public final void setError(IOException exception) throws IOException {
    Objects.requireNonNull(exception, "exception required");
    errorMessage = exception.getMessage();
    // StringWriter sw = new StringWriter();
    // exception.printStackTrace(new PrintWriter(sw));
    // errorMessage+="\n====\n"+sw.toString();
    errorStatus = true;
    throw exception;
  }

  public final void setError(String error) {
    this.errorMessage = Objects.requireNonNull(error, "error required");
    this.errorStatus = true;
  }

  public final void setFinished() {
    if (!finished) {
      totalTime = ((Long) (System.currentTimeMillis() - startTime)).intValue();
      shardInfoUpdated = false;
      finished = true;
      rsp = null;
    }
  }

  public void setKey(String key) throws IOException {
    if (this.key != null) {
      throw new IOException("key already set");
    } else {
      this.key = Objects.requireNonNull(key, "key required");
    }
  }

  public final boolean shardRequest() {
    return shardRequest;
  }

  public final Map<String, ShardStatus> getShards() {
    return shards;
  }

  public final Status status() {
    return status;
  }

  public final boolean error() {
    return errorStatus;
  }

  public final boolean abort() {
    return abortStatus;
  }

  public final boolean finished() {
    return finished;
  }

  public final String abortMessage() {
    return Objects.requireNonNull(abortMessage, "no abortMessage available");
  }

  public final String errorMessage() {
    return Objects.requireNonNull(errorMessage, "no errorMessage available");
  }

  public final Long getStartTime() {
    return startTime;
  }

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

  public SimpleOrderedMap<Object> createListOutput() {
    return createOutput(false);
  }

  public SimpleOrderedMap<Object> createItemOutput() {
    return createOutput(true);
  }

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

  private final Integer getInteger(NamedList<Object> response, String... args) {
    Object objectItem = response.findRecursive(args);
    if (objectItem != null && objectItem instanceof Integer) {
      return (Integer) objectItem;
    } else {
      return null;
    }
  }

  private final Map<String, Integer> getIntegerMap(NamedList<Object> response, String... args) {
    Object objectItem = response.findRecursive(args);
    Map<String, Integer> result = null;
    if (objectItem != null && objectItem instanceof Map) {
      result = (Map) objectItem;
    }
    return result;
  }

  private final Long getLong(NamedList<Object> response, String... args) {
    Object objectItem = response.findRecursive(args);
    if (objectItem != null && objectItem instanceof Long) {
      return (Long) objectItem;
    } else {
      return null;
    }
  }

  private final Map<String, Long> getLongMap(NamedList<Object> response, String... args) {
    Object objectItem = response.findRecursive(args);
    if (objectItem != null && objectItem instanceof Map) {
      return (Map) objectItem;
    } else {
      return null;
    }
  }

  private final String getString(NamedList<Object> response, String... args) {
    Object objectItem = response.findRecursive(args);
    if (objectItem != null && objectItem instanceof String) {
      return (String) objectItem;
    } else {
      return null;
    }
  }

  private final Boolean getBoolean(NamedList<Object> response, String... args) {
    Object objectItem = response.findRecursive(args);
    Boolean result = null;
    if (objectItem != null && objectItem instanceof Boolean) {
      result = (Boolean) objectItem;
    }
    return result;
  }

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

  @Override
  public String toString() {
    return createItemOutput().toString();
  }

  public static class StageStatus {
    private int stage;

    public boolean checked;
    public boolean finished;
    public int numberOfDocumentsFound;
    public int numberOfShards;

    public StageStatus(int stage) {
      this.stage = stage;
      reset();
    }

    public void reset() {
      finished = true;
      checked = false;
      numberOfDocumentsFound = 0;
      numberOfShards = 0;
    }

    public void add(boolean finished, Long numberOfDocumentsFound) {
      if (!finished) {
        this.finished = false;
      }
      if (numberOfDocumentsFound != null) {
        this.numberOfDocumentsFound += numberOfDocumentsFound;
      }
      numberOfShards++;
    }

    public SimpleOrderedMap<Object> createOutput() {
      SimpleOrderedMap<Object> stageOutput = new SimpleOrderedMap<>();
      stageOutput.add(NAME_FINISHED, finished);
      stageOutput.add(NAME_STATUS_DOCUMENT_NUMBER_FOUND, numberOfDocumentsFound);
      return stageOutput;
    }

    public int stage() {
      return stage;
    }
  }

  public class ShardStatus {
    public String name;
    public String location;
    public String mtasHandler;
    public Integer numberSegmentsTotal = null;
    public Long numberDocumentsTotal = null;
    public Map<Integer, Long> numberDocumentsFoundStage = new HashMap<>();
    public Map<Integer, String> errorStage = null;
    public Map<Integer, String> abortStage = null;
    public Integer stage = null;
    public Map<Integer, Integer> timeStage = new HashMap<>();
    public Set<Integer> finishedStages = new HashSet<>();
    public Integer stageNumberSegmentsFinished = null;
    public Integer stageSubNumberSegmentsTotal = null;
    public Integer stageSubNumberSegmentsFinishedTotal = null;
    public Map<String, Integer> stageSubNumberSegmentsFinished = new HashMap<>();
    public Long stageNumberDocumentsFinished = null;
    public Long stageSubNumberDocumentsTotal = null;
    public Long stageSubNumberDocumentsFinishedTotal = null;
    public Map<String, Long> stageSubNumberDocumentsFinished = new HashMap<>();

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
