/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.txn.compactor;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;

import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.StringableMap;
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.DriverUtils;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.exec.mr3.DAGUtils;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils.Directory;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatusWithId;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to do compactions via an MR job.  This has to be in the ql package rather than metastore
 * .compactions package with all of it's relatives because it needs access to the actual input
 * and output formats, which are in ql.  ql depends on metastore and we can't have a circular
 * dependency.
 */
public class CompactorMR {

  static final private String CLASS_NAME = CompactorMR.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  static final private String INPUT_FORMAT_CLASS_NAME = "hive.compactor.input.format.class.name";
  static final private String OUTPUT_FORMAT_CLASS_NAME = "hive.compactor.output.format.class.name";
  static final private String TMP_LOCATION = "hive.compactor.input.tmp.dir";
  static final private String FINAL_LOCATION = "hive.compactor.input.dir";
  static final private String MIN_TXN = "hive.compactor.txn.min";
  static final private String MAX_TXN = "hive.compactor.txn.max";
  static final private String IS_MAJOR = "hive.compactor.is.major";
  static final private String IS_COMPRESSED = "hive.compactor.is.compressed";
  static final private String TABLE_PROPS = "hive.compactor.table.props";
  static final private String NUM_BUCKETS = hive_metastoreConstants.BUCKET_COUNT;
  static final private String BASE_DIR = "hive.compactor.base.dir";
  static final private String DELTA_DIRS = "hive.compactor.delta.dirs";
  static final private String DIRS_TO_SEARCH = "hive.compactor.dirs.to.search";
  static final private String TMPDIR = "_tmp";
  static final private String TBLPROPS_PREFIX = "tblprops.";
  static final private String COMPACTOR_PREFIX = "compactor.";

  private JobConf mrJob;  // MR job for compaction

  public CompactorMR() {
  }

  private JobConf createBaseJobConf(HiveConf conf, String jobName, Table t, StorageDescriptor sd,
                                    ValidWriteIdList writeIds, CompactionInfo ci) {
    JobConf job = new JobConf(conf);
    job.setJobName(jobName);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setJarByClass(CompactorMR.class);
    LOG.debug("User jar set to " + job.getJar());
    job.setMapperClass(CompactorMap.class);
    job.setNumReduceTasks(0);
    job.setInputFormat(CompactorInputFormat.class);
    job.setOutputFormat(NullOutputFormat.class);
    job.setOutputCommitter(CompactorOutputCommitter.class);

    String queueName = conf.getVar(HiveConf.ConfVars.COMPACTOR_JOB_QUEUE);
    if(queueName != null && queueName.length() > 0) {
      job.setQueueName(queueName);
    }

    job.set(FINAL_LOCATION, sd.getLocation());
    job.set(TMP_LOCATION, generateTmpPath(sd));
    job.set(INPUT_FORMAT_CLASS_NAME, sd.getInputFormat());
    job.set(OUTPUT_FORMAT_CLASS_NAME, sd.getOutputFormat());
    job.setBoolean(IS_COMPRESSED, sd.isCompressed());
    job.set(TABLE_PROPS, new StringableMap(t.getParameters()).toString());
    job.setInt(NUM_BUCKETS, sd.getNumBuckets());
    job.set(ValidWriteIdList.VALID_WRITEIDS_KEY, writeIds.toString());
    overrideMRProps(job, t.getParameters()); // override MR properties from tblproperties if applicable
    if (ci.properties != null) {
      overrideTblProps(job, t.getParameters(), ci.properties);
    }
    setColumnTypes(job, sd.getCols());
    //with feature on, multiple tasks may get into conflict creating/using TMP_LOCATION and if we were
    //to generate the target dir in the Map task, there is no easy way to pass it to OutputCommitter
    //to do the final move
    job.setBoolean("mapreduce.map.speculative", false);

    // Set appropriate Acid readers/writers based on the table properties.
    AcidUtils.setAcidOperationalProperties(job, true,
            AcidUtils.getAcidOperationalProperties(t.getParameters()));

    return job;
  }

  /**
   * Parse tblproperties specified on "ALTER TABLE ... COMPACT ... WITH OVERWRITE TBLPROPERTIES ..."
   * and override two categories of properties:
   * 1. properties of the compactor MR job (with prefix "compactor.")
   * 2. general hive properties (with prefix "tblprops.")
   * @param job the compactor MR job
   * @param tblproperties existing tblproperties
   * @param properties table properties
   */
  private void overrideTblProps(JobConf job, Map<String, String> tblproperties, String properties) {
    StringableMap stringableMap = new StringableMap(properties);
    overrideMRProps(job, stringableMap);
    // mingle existing tblproperties with those specified on the ALTER TABLE command
    for (String key : stringableMap.keySet()) {
      if (key.startsWith(TBLPROPS_PREFIX)) {
        String propKey = key.substring(9);  // 9 is the length of "tblprops.". We only keep the rest
        tblproperties.put(propKey, stringableMap.get(key));
      }
    }
    // re-set TABLE_PROPS with reloaded tblproperties
    job.set(TABLE_PROPS, new StringableMap(tblproperties).toString());
  }

  /**
   * Parse tblproperties to override relevant properties of compactor MR job with specified values.
   * For example, compactor.mapreuce.map.memory.mb=1024
   * @param job the compactor MR job
   * @param properties table properties
   */
  private void overrideMRProps(JobConf job, Map<String, String> properties) {
    for (String key : properties.keySet()) {
      if (key.startsWith(COMPACTOR_PREFIX)) {
        String mrKey = key.substring(10); // 10 is the length of "compactor." We only keep the rest.
        job.set(mrKey, properties.get(key));
      }
    }
  }

  /**
   * Run Compaction which may consist of several jobs on the cluster.
   * @param conf Hive configuration file
   * @param jobName name to run this job with
   * @param t metastore table
   * @param sd metastore storage descriptor
   * @param writeIds list of valid write ids
   * @param ci CompactionInfo
   * @throws java.io.IOException if the job fails
   */
  void run(HiveConf conf, String jobName, Table t, Partition p, StorageDescriptor sd, ValidWriteIdList writeIds,
           CompactionInfo ci, Worker.StatsUpdater su, TxnStore txnHandler) throws IOException {

    if(conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST) && conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODEFAILCOMPACTION)) {
      throw new RuntimeException(HiveConf.ConfVars.HIVETESTMODEFAILCOMPACTION.name() + "=true");
    }

    if (AcidUtils.isInsertOnlyTable(t.getParameters())) {
      if (HiveConf.getBoolVar(conf, ConfVars.HIVE_COMPACTOR_COMPACT_MM)) {
        runMmCompaction(conf, t, p, sd, writeIds, ci);
      }
      return;
    }

    JobConf job = createBaseJobConf(conf, jobName, t, sd, writeIds, ci);

    // Figure out and encode what files we need to read.  We do this here (rather than in
    // getSplits below) because as part of this we discover our minimum and maximum transactions,
    // and discovering that in getSplits is too late as we then have no way to pass it to our
    // mapper.

    AcidUtils.Directory dir = AcidUtils.getAcidState(null, new Path(sd.getLocation()), conf, writeIds, false, true);
    List<AcidUtils.ParsedDelta> parsedDeltas = dir.getCurrentDirectories();
    int maxDeltastoHandle = conf.getIntVar(HiveConf.ConfVars.COMPACTOR_MAX_NUM_DELTA);
    if(parsedDeltas.size() > maxDeltastoHandle) {
      /**
       * if here, that means we have very high number of delta files.  This may be sign of a temporary
       * glitch or a real issue.  For example, if transaction batch size or transaction size is set too
       * low for the event flow rate in Streaming API, it may generate lots of delta files very
       * quickly.  Another possibility is that Compaction is repeatedly failing and not actually compacting.
       * Thus, force N minor compactions first to reduce number of deltas and then follow up with
       * the compaction actually requested in {@link ci} which now needs to compact a lot fewer deltas
       */
      LOG.warn(parsedDeltas.size() + " delta files found for " + ci.getFullPartitionName()
        + " located at " + sd.getLocation() + "! This is likely a sign of misconfiguration, " +
        "especially if this message repeats.  Check that compaction is running properly.  Check for any " +
        "runaway/mis-configured process writing to ACID tables, especially using Streaming Ingest API.");
      int numMinorCompactions = parsedDeltas.size() / maxDeltastoHandle;
      for(int jobSubId = 0; jobSubId < numMinorCompactions; jobSubId++) {
        JobConf jobMinorCompact = createBaseJobConf(conf, jobName + "_" + jobSubId, t, sd, writeIds, ci);
        launchCompactionJob(jobMinorCompact,
          null, CompactionType.MINOR, null,
          parsedDeltas.subList(jobSubId * maxDeltastoHandle, (jobSubId + 1) * maxDeltastoHandle),
          maxDeltastoHandle, -1, conf, txnHandler, ci.id, jobName);
      }
      //now recompute state since we've done minor compactions and have different 'best' set of deltas
      dir = AcidUtils.getAcidState(new Path(sd.getLocation()), conf, writeIds);
    }

    StringableList dirsToSearch = new StringableList();
    Path baseDir = null;
    if (ci.isMajorCompaction()) {
      // There may not be a base dir if the partition was empty before inserts or if this
      // partition is just now being converted to ACID.
      baseDir = dir.getBaseDirectory();
      if (baseDir == null) {
        List<HdfsFileStatusWithId> originalFiles = dir.getOriginalFiles();
        if (!(originalFiles == null) && !(originalFiles.size() == 0)) {
          // There are original format files
          for (HdfsFileStatusWithId stat : originalFiles) {
            Path path = stat.getFileStatus().getPath();
            //note that originalFiles are all original files recursively not dirs
            dirsToSearch.add(path);
            LOG.debug("Adding original file " + path + " to dirs to search");
          }
          // Set base to the location so that the input format reads the original files.
          baseDir = new Path(sd.getLocation());
        }
      } else {
        // add our base to the list of directories to search for files in.
        LOG.debug("Adding base directory " + baseDir + " to dirs to search");
        dirsToSearch.add(baseDir);
      }
    }
    if (parsedDeltas.size() == 0 && dir.getOriginalFiles().size() == 0) {
      // Skip compaction if there's no delta files AND there's no original files
      String minOpenInfo = ".";
      if(writeIds.getMinOpenWriteId() != null) {
        minOpenInfo = " with min Open " + JavaUtils.writeIdToString(writeIds.getMinOpenWriteId()) +
          ".  Compaction cannot compact above this writeId";
      }
      LOG.error("No delta files or original files found to compact in " + sd.getLocation() +
        " for compactionId=" + ci.id + minOpenInfo);
      return;
    }

    launchCompactionJob(job, baseDir, ci.type, dirsToSearch, dir.getCurrentDirectories(),
      dir.getCurrentDirectories().size(), dir.getObsolete().size(), conf, txnHandler, ci.id, jobName);

    su.gatherStats();
  }

  private void runMmCompaction(HiveConf conf, Table t, Partition p,
      StorageDescriptor sd, ValidWriteIdList writeIds, CompactionInfo ci) throws IOException {
    LOG.debug("Going to delete directories for aborted transactions for MM table "
        + t.getDbName() + "." + t.getTableName());
    AcidUtils.Directory dir = AcidUtils.getAcidState(null, new Path(sd.getLocation()),
        conf, writeIds, Ref.from(false), false, t.getParameters());
    removeFilesForMmTable(conf, dir);

    // Then, actually do the compaction.
    if (!ci.isMajorCompaction()) {
      // Not supported for MM tables right now.
      LOG.info("Not compacting " + sd.getLocation() + "; not a major compaction");
      return;
    }

    int deltaCount = dir.getCurrentDirectories().size();
    int origCount = dir.getOriginalFiles().size();
    if ((deltaCount + (dir.getBaseDirectory() == null ? 0 : 1)) + origCount <= 1) {
      LOG.debug("Not compacting " + sd.getLocation() + "; current base is "
        + dir.getBaseDirectory() + " and there are " + deltaCount + " deltas and "
        + origCount + " originals");
      return;
    }
    try {
      String tmpLocation = generateTmpPath(sd);
      Path baseLocation = new Path(tmpLocation, "_base");

      // Set up the session for driver.
      conf = new HiveConf(conf);
      conf.set(ConfVars.HIVE_QUOTEDID_SUPPORT.varname, "column");

      String user = UserGroupInformation.getCurrentUser().getShortUserName();
      SessionState sessionState = DriverUtils.setUpSessionState(conf, user, false);

      // Note: we could skip creating the table and just add table type stuff directly to the
      //       "insert overwrite directory" command if there were no bucketing or list bucketing.
      String tmpPrefix = t.getDbName() + ".tmp_compactor_" + t.getTableName() + "_";
      String tmpTableName = null;
      while (true) {
        tmpTableName = tmpPrefix + System.currentTimeMillis();
        String query = buildMmCompactionCtQuery(tmpTableName, t,
            p == null ? t.getSd() : p.getSd(), baseLocation.toString());
        LOG.info("Compacting a MM table into " + query);
        try {
          DriverUtils.runOnDriver(conf, user, sessionState, query, null);
          break;
        } catch (Exception ex) {
          Throwable cause = ex;
          while (cause != null && !(cause instanceof AlreadyExistsException)) {
            cause = cause.getCause();
          }
          if (cause == null) {
            throw new IOException(ex);
          }
        }
      }

      String query = buildMmCompactionQuery(conf, t, p, tmpTableName);
      LOG.info("Compacting a MM table via " + query);
      DriverUtils.runOnDriver(conf, user, sessionState, query, writeIds);
      commitMmCompaction(tmpLocation, sd.getLocation(), conf, writeIds);
      DriverUtils.runOnDriver(conf, user, sessionState,
          "drop table if exists " + tmpTableName, null);
    } catch (HiveException e) {
      LOG.error("Error compacting a MM table", e);
      throw new IOException(e);
    }
  }

  private String generateTmpPath(StorageDescriptor sd) {
    return sd.getLocation() + "/" + TMPDIR + "_" + UUID.randomUUID().toString();
  }

  private String buildMmCompactionCtQuery(
      String fullName, Table t, StorageDescriptor sd, String location) {
    StringBuilder query = new StringBuilder("create temporary table ")
      .append(fullName).append("(");
    List<FieldSchema> cols = t.getSd().getCols();
    boolean isFirst = true;
    for (FieldSchema col : cols) {
      if (!isFirst) {
        query.append(", ");
      }
      isFirst = false;
      query.append("`").append(col.getName()).append("` ").append(col.getType());
    }
    query.append(") ");

    // Bucketing.
    List<String> buckCols = t.getSd().getBucketCols();
    if (buckCols.size() > 0) {
      query.append("CLUSTERED BY (").append(StringUtils.join(",", buckCols)).append(") ");
      List<Order> sortCols = t.getSd().getSortCols();
      if (sortCols.size() > 0) {
        query.append("SORTED BY (");
        List<String> sortKeys = new ArrayList<String>();
        isFirst = true;
        for (Order sortCol : sortCols) {
          if (!isFirst) {
            query.append(", ");
          }
          isFirst = false;
          query.append(sortCol.getCol()).append(" ");
          if (sortCol.getOrder() == BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_ASC) {
            query.append("ASC");
          } else if (sortCol.getOrder() == BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_DESC) {
            query.append("DESC");
          }
        }
        query.append(") ");
      }
      query.append("INTO ").append(t.getSd().getNumBuckets()).append(" BUCKETS");
    }

    // Stored as directories. We don't care about the skew otherwise.
    if (t.getSd().isStoredAsSubDirectories()) {
      SkewedInfo skewedInfo = t.getSd().getSkewedInfo();
      if (skewedInfo != null && !skewedInfo.getSkewedColNames().isEmpty()) {
        query.append(" SKEWED BY (").append(
            StringUtils.join(", ", skewedInfo.getSkewedColNames())).append(") ON ");
        isFirst = true;
        for (List<String> colValues : skewedInfo.getSkewedColValues()) {
          if (!isFirst) {
            query.append(", ");
          }
          isFirst = false;
          query.append("('").append(StringUtils.join("','", colValues)).append("')");
        }
        query.append(") STORED AS DIRECTORIES");
      }
    }

    SerDeInfo serdeInfo = sd.getSerdeInfo();
    Map<String, String> serdeParams = serdeInfo.getParameters();
    query.append(" ROW FORMAT SERDE '").append(HiveStringUtils.escapeHiveCommand(
        serdeInfo.getSerializationLib())).append("'");
    String sh = t.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE);
    assert sh == null; // Not supposed to be a compactable table.
    if (!serdeParams.isEmpty()) {
      DDLTask.appendSerdeParams(query, serdeParams);
    }
    query.append("STORED AS INPUTFORMAT '").append(
        HiveStringUtils.escapeHiveCommand(sd.getInputFormat())).append("' OUTPUTFORMAT '").append(
        HiveStringUtils.escapeHiveCommand(sd.getOutputFormat())).append("' LOCATION '").append(
        HiveStringUtils.escapeHiveCommand(location)).append("' TBLPROPERTIES (");
    // Exclude all standard table properties.
    Set<String> excludes = getHiveMetastoreConstants();
    excludes.addAll(Lists.newArrayList(StatsSetupConst.TABLE_PARAMS_STATS_KEYS));
    isFirst = true;
    for (Map.Entry<String, String> e : t.getParameters().entrySet()) {
      if (e.getValue() == null) continue;
      if (excludes.contains(e.getKey())) continue;
      if (!isFirst) {
        query.append(", ");
      }
      isFirst = false;
      query.append("'").append(e.getKey()).append("'='").append(
          HiveStringUtils.escapeHiveCommand(e.getValue())).append("'");
    }
    if (!isFirst) {
      query.append(", ");
    }
    query.append("'transactional'='false')");
    return query.toString();

  }

  private static Set<String> getHiveMetastoreConstants() {
    HashSet<String> result = new HashSet<>();
    for (Field f : hive_metastoreConstants.class.getDeclaredFields()) {
      if (!Modifier.isStatic(f.getModifiers())) continue;
      if (!Modifier.isFinal(f.getModifiers())) continue;
      if (!String.class.equals(f.getType())) continue;
      f.setAccessible(true);
      try {
        result.add((String)f.get(null));
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }

  private String buildMmCompactionQuery(HiveConf conf, Table t, Partition p, String tmpName) {
    String fullName = t.getDbName() + "." + t.getTableName();
    // TODO: ideally we should make a special form of insert overwrite so that we:
    //       1) Could use fast merge path for ORC and RC.
    //       2) Didn't have to create a table.

    String query = "insert overwrite table " + tmpName + " ";
    String filter = "";
    if (p != null) {
      filter = " where ";
      List<String> vals = p.getValues();
      List<FieldSchema> keys = t.getPartitionKeys();
      assert keys.size() == vals.size();
      for (int i = 0; i < keys.size(); ++i) {
        filter += (i == 0 ? "`" : " and `") + (keys.get(i).getName() + "`='" + vals.get(i) + "'");
      }
      query += " select ";
      // Use table descriptor for columns.
      List<FieldSchema> cols = t.getSd().getCols();
      for (int i = 0; i < cols.size(); ++i) {
        query += (i == 0 ? "`" : ", `") + (cols.get(i).getName() + "`");
      }
    } else {
      query += "select *";
    }
    query += " from "  + fullName + filter;
    return query;
  }

  /**
   * @param baseDir if not null, it's either table/partition root folder or base_xxxx.
   *                If it's base_xxxx, it's in dirsToSearch, else the actual original files
   *                (all leaves recursively) are in the dirsToSearch list
   */
  private void launchCompactionJob(JobConf job, Path baseDir, CompactionType compactionType,
                                   StringableList dirsToSearch,
                                   List<AcidUtils.ParsedDelta> parsedDeltas,
                                   int curDirNumber, int obsoleteDirNumber, HiveConf hiveConf,
                                   TxnStore txnHandler, long id, String jobName) throws IOException {
    job.setBoolean(IS_MAJOR, compactionType == CompactionType.MAJOR);
    if(dirsToSearch == null) {
      dirsToSearch = new StringableList();
    }
    StringableList deltaDirs = new StringableList();
    // Note: if compaction creates a delta, it won't replace an existing base dir, so the txn ID
    //       of the base dir won't be a part of delta's range. If otoh compaction creates a base,
    //       we don't care about this value because bases don't have min txn ID in the name.
    //       However logically this should also take base into account if it's included.
    long minTxn = Long.MAX_VALUE;
    long maxTxn = Long.MIN_VALUE;
    for (AcidUtils.ParsedDelta delta : parsedDeltas) {
      LOG.debug("Adding delta " + delta.getPath() + " to directories to search");
      dirsToSearch.add(delta.getPath());
      deltaDirs.add(delta.getPath());
      minTxn = Math.min(minTxn, delta.getMinWriteId());
      maxTxn = Math.max(maxTxn, delta.getMaxWriteId());
    }

    if (baseDir != null) job.set(BASE_DIR, baseDir.toString());
    job.set(DELTA_DIRS, deltaDirs.toString());
    job.set(DIRS_TO_SEARCH, dirsToSearch.toString());
    job.setLong(MIN_TXN, minTxn);
    job.setLong(MAX_TXN, maxTxn);

    // Add tokens for all the file system in the input path.
    if (DAGUtils.getInstance().shouldAddPathsToCredentials(job)) {
      ArrayList<Path> dirs = new ArrayList<>();
      if (baseDir != null) {
        dirs.add(baseDir);
      }
      dirs.addAll(deltaDirs);
      dirs.addAll(dirsToSearch);
      TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs.toArray(new Path[]{}), job);
    }

    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)) {
      mrJob = job;
    }

    boolean submitJobUsingMr3 = hiveConf.getBoolVar(ConfVars.HIVE_MR3_COMPACTION_USING_MR3);
    if (submitJobUsingMr3) {
      try {
        job.setJobName("MR3-compaction-" + id);
        LOG.info("Submitting " + compactionType + " compaction job '" +
              job.getJobName() + "' to MR3 " +
              "(current delta dirs count=" + curDirNumber +
              ", obsolete delta dirs count=" + obsoleteDirNumber + ". TxnIdRange[" + minTxn + "," + maxTxn + "]");
        txnHandler.setHadoopJobId(job.getJobName(), id);
        // Each compaction job creates its own MR3CompactionHelper and discards it because:
        //  1. the retry logic is already implemented inside the compaction thread itself.
        //  2. MR3CompactionHelper is not created frequently.
        new MR3CompactionHelper(hiveConf).submitJobToMr3(job);
      } catch (Exception e) {
        LOG.info("Compaction using MR3 failed. Retrying compaction using MR", e);
        submitJobUsingMr3 = false;
      }
    }

    if (!submitJobUsingMr3) {
      job.setJobName("MR-compaction-" + id);
      LOG.info("Submitting " + compactionType + " compaction job '" +
              job.getJobName() + "' to " + job.getQueueName() + " queue.  " +
              "(current delta dirs count=" + curDirNumber +
              ", obsolete delta dirs count=" + obsoleteDirNumber + ". TxnIdRange[" + minTxn + "," + maxTxn + "]");
      JobClient jc = null;
      try {
        jc = new JobClient(job);
        RunningJob rj = jc.submitJob(job);
        LOG.info("Submitted compaction job '" + job.getJobName() +
                "' with jobID=" + rj.getID() + " compaction ID=" + id);
        txnHandler.setHadoopJobId(rj.getID().toString(), id);
        rj.waitForCompletion();
        if (!rj.isSuccessful()) {
          throw new IOException((compactionType == CompactionType.MAJOR ? "Major" : "Minor") +
                  " compactor job failed for " + jobName + "! Hadoop JobId: " + rj.getID());
        }
      } finally {
        if (jc != null) {
          jc.close();
        }
      }
    }
  }

  /**
   * Set the column names and types into the job conf for the input format
   * to use.
   * @param job the job to update
   * @param cols the columns of the table
   * @param map
   */
  private void setColumnTypes(JobConf job, List<FieldSchema> cols) {
    StringBuilder colNames = new StringBuilder();
    StringBuilder colTypes = new StringBuilder();
    boolean isFirst = true;
    for(FieldSchema col: cols) {
      if (isFirst) {
        isFirst = false;
      } else {
        colNames.append(',');
        colTypes.append(',');
      }
      colNames.append(col.getName());
      colTypes.append(col.getType());
    }
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, colNames.toString());
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, colTypes.toString());
    HiveConf.setVar(job, HiveConf.ConfVars.HIVEINPUTFORMAT, HiveInputFormat.class.getName());
  }

  // Remove the directories for aborted transactions only
  private void removeFilesForMmTable(HiveConf conf, Directory dir) throws IOException {
    // For MM table, we only want to delete delta dirs for aborted txns.
    List<FileStatus> abortedDirs = dir.getAbortedDirectories();
    List<Path> filesToDelete = new ArrayList<>(abortedDirs.size());
    for (FileStatus stat : abortedDirs) {
      filesToDelete.add(stat.getPath());
    }
    if (filesToDelete.size() < 1) {
      return;
    }
    LOG.info("About to remove " + filesToDelete.size() + " aborted directories from " + dir);
    FileSystem fs = filesToDelete.get(0).getFileSystem(conf);
    for (Path dead : filesToDelete) {
      LOG.debug("Going to delete path " + dead.toString());
      fs.delete(dead, true);
    }
  }

  public JobConf getMrJob() {
    return mrJob;
  }

  static class CompactorInputSplit implements InputSplit {
    private long length = 0;
    private List<String> locations;
    private int bucketNum;
    private Path base;
    private Path[] deltas;

    public CompactorInputSplit() {
    }

    /**
     *
     * @param hadoopConf
     * @param bucket bucket to be processed by this split
     * @param files actual files this split should process.  It is assumed the caller has already
     *              parsed out the files in base and deltas to populate this list.  Includes copy_N
     * @param base directory of the base, or the partition/table location if the files are in old
     *             style.  Can be null.
     * @param deltas directories of the delta files.
     * @throws IOException
     */
    CompactorInputSplit(Configuration hadoopConf, int bucket, List<Path> files, Path base,
                               Path[] deltas)
        throws IOException {
      bucketNum = bucket;
      this.base = base;
      this.deltas = deltas;
      locations = new ArrayList<String>();

      for (Path path : files) {
        FileSystem fs = path.getFileSystem(hadoopConf);
        FileStatus stat = fs.getFileStatus(path);
        length += stat.getLen();
        BlockLocation[] locs = fs.getFileBlockLocations(stat, 0, length);
        for (int i = 0; i < locs.length; i++) {
          String[] hosts = locs[i].getHosts();
          for (int j = 0; j < hosts.length; j++) {
            locations.add(hosts[j]);
          }
        }
      }
    }

    @Override
    public long getLength() throws IOException {
      return length;
    }

    @Override
    public String[] getLocations() throws IOException {
      return locations.toArray(new String[locations.size()]);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeLong(length);
      dataOutput.writeInt(locations.size());
      for (int i = 0; i < locations.size(); i++) {
        dataOutput.writeInt(locations.get(i).length());
        dataOutput.writeBytes(locations.get(i));
      }
      dataOutput.writeInt(bucketNum);
      if (base == null) {
        dataOutput.writeInt(0);
      } else {
        dataOutput.writeInt(base.toString().length());
        dataOutput.writeBytes(base.toString());
      }
      dataOutput.writeInt(deltas.length);
      for (int i = 0; i < deltas.length; i++) {
        dataOutput.writeInt(deltas[i].toString().length());
        dataOutput.writeBytes(deltas[i].toString());
      }

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      int len;
      byte[] buf;

      locations = new ArrayList<String>();
      length = dataInput.readLong();
      LOG.debug("Read length of " + length);
      int numElements = dataInput.readInt();
      LOG.debug("Read numElements of " + numElements);
      for (int i = 0; i < numElements; i++) {
        len = dataInput.readInt();
        LOG.debug("Read file length of " + len);
        buf = new byte[len];
        dataInput.readFully(buf);
        locations.add(new String(buf));
      }
      bucketNum = dataInput.readInt();
      LOG.debug("Read bucket number of " + bucketNum);
      len = dataInput.readInt();
      LOG.debug("Read base path length of " + len);
      if (len > 0) {
        buf = new byte[len];
        dataInput.readFully(buf);
        base = new Path(new String(buf));
      }
      numElements = dataInput.readInt();
      deltas = new Path[numElements];
      for (int i = 0; i < numElements; i++) {
        len = dataInput.readInt();
        buf = new byte[len];
        dataInput.readFully(buf);
        deltas[i] = new Path(new String(buf));
      }
    }

    public void set(CompactorInputSplit other) {
      length = other.length;
      locations = other.locations;
      bucketNum = other.bucketNum;
      base = other.base;
      deltas = other.deltas;
    }

    int getBucket() {
      return bucketNum;
    }

    Path getBaseDir() {
      return base;
    }

    Path[] getDeltaDirs() {
      return deltas;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("CompactorInputSplit{base: ");
      builder.append(base);
      builder.append(", bucket: ");
      builder.append(bucketNum);
      builder.append(", length: ");
      builder.append(length);
      builder.append(", deltas: [");
      for(int i=0; i < deltas.length; ++i) {
        if (i != 0) {
          builder.append(", ");
        }
        builder.append(deltas[i].getName());
      }
      builder.append("]}");
      return builder.toString();
    }
  }

  /**
   * This input format returns its own input split as a value.  This is because our splits
   * contain information needed to properly construct the writer.  Crazy, huh?
   */
  static class CompactorInputFormat implements InputFormat<NullWritable, CompactorInputSplit> {

    @Override
    public InputSplit[] getSplits(JobConf entries, int i) throws IOException {
      Path baseDir = null;
      if (entries.get(BASE_DIR) != null) baseDir = new Path(entries.get(BASE_DIR));
      StringableList tmpDeltaDirs = new StringableList(entries.get(DELTA_DIRS));
      Path[] deltaDirs = tmpDeltaDirs.toArray(new Path[tmpDeltaDirs.size()]);
      StringableList dirsToSearch = new StringableList(entries.get(DIRS_TO_SEARCH));
      Map<Integer, BucketTracker> splitToBucketMap = new HashMap<Integer, BucketTracker>();
      for (Path dir : dirsToSearch) {
        FileSystem fs = dir.getFileSystem(entries);
        // When we have split-update and there are two kinds of delta directories-
        // the delta_x_y/ directory one which has only insert events and
        // the delete_delta_x_y/ directory which has only the delete events.
        // The clever thing about this kind of splitting is that everything in the delta_x_y/
        // directory can be processed as base files. However, this is left out currently
        // as an improvement for the future.

        if (dir.getName().startsWith(AcidUtils.BASE_PREFIX) ||
            dir.getName().startsWith(AcidUtils.DELTA_PREFIX) ||
            dir.getName().startsWith(AcidUtils.DELETE_DELTA_PREFIX)) {
          boolean sawBase = dir.getName().startsWith(AcidUtils.BASE_PREFIX);
          boolean isRawFormat = !dir.getName().startsWith(AcidUtils.DELETE_DELTA_PREFIX)
            && AcidUtils.MetaDataFile.isRawFormat(dir, fs);//deltes can't be raw format

          FileStatus[] files = fs.listStatus(dir, isRawFormat ? AcidUtils.originalBucketFilter
            : AcidUtils.bucketFileFilter);
          for(FileStatus f : files) {
            // For each file, figure out which bucket it is.
            Matcher matcher = isRawFormat ?
              AcidUtils.LEGACY_BUCKET_DIGIT_PATTERN.matcher(f.getPath().getName())
              : AcidUtils.BUCKET_DIGIT_PATTERN.matcher(f.getPath().getName());
            addFileToMap(matcher, f.getPath(), sawBase, splitToBucketMap);
          }
        } else {
          // Legacy file, see if it's a bucket file
          Matcher matcher = AcidUtils.LEGACY_BUCKET_DIGIT_PATTERN.matcher(dir.getName());
          addFileToMap(matcher, dir, true, splitToBucketMap);
        }
      }


      List<InputSplit> splits = new ArrayList<InputSplit>(splitToBucketMap.size());
      for (Map.Entry<Integer, BucketTracker> e : splitToBucketMap.entrySet()) {
        BucketTracker bt = e.getValue();
        splits.add(new CompactorInputSplit(entries, e.getKey(), bt.buckets,
            bt.sawBase ? baseDir : null, deltaDirs));
      }

      LOG.debug("Returning " + splits.size() + " splits");
      return splits.toArray(new InputSplit[splits.size()]);
    }

    @Override
    public RecordReader<NullWritable, CompactorInputSplit> getRecordReader(
        InputSplit inputSplit,  JobConf entries, Reporter reporter) throws IOException {
      return new CompactorRecordReader((CompactorInputSplit)inputSplit);
    }

    private void addFileToMap(Matcher matcher, Path file, boolean sawBase,
                              Map<Integer, BucketTracker> splitToBucketMap) {
      if (!matcher.find()) {
        String msg = "Found a non-bucket file that we thought matched the bucket pattern! " +
          file.toString() + " Matcher=" + matcher.toString();
        LOG.error(msg);
        //following matcher.group() would fail anyway and we don't want to skip files since that
        //may be a data loss scenario
        throw new IllegalArgumentException(msg);
      }
      int bucketNum = Integer.parseInt(matcher.group());
      BucketTracker bt = splitToBucketMap.get(bucketNum);
      if (bt == null) {
        bt = new BucketTracker();
        splitToBucketMap.put(bucketNum, bt);
      }
      LOG.debug("Adding " + file.toString() + " to list of files for splits");
      bt.buckets.add(file);
      bt.sawBase |= sawBase;
    }

    private static class BucketTracker {
      BucketTracker() {
        sawBase = false;
        buckets = new ArrayList<Path>();
      }

      boolean sawBase;
      List<Path> buckets;
    }
  }

  static class CompactorRecordReader
      implements RecordReader<NullWritable, CompactorInputSplit> {
    private CompactorInputSplit split;

    CompactorRecordReader(CompactorInputSplit split) {
      this.split = split;
    }

    @Override
    public boolean next(NullWritable key,
                        CompactorInputSplit compactorInputSplit) throws IOException {
      if (split != null) {
        compactorInputSplit.set(split);
        split = null;
        return true;
      }
      return false;
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public CompactorInputSplit createValue() {
      return new CompactorInputSplit();
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public float getProgress() throws IOException {
      return 0;
    }
  }

  static class CompactorMap<V extends Writable>
      implements Mapper<WritableComparable, CompactorInputSplit,  NullWritable,  NullWritable> {

    JobConf jobConf;
    RecordWriter writer = null;
    RecordWriter deleteEventWriter = null;

    @Override
    public void map(WritableComparable key, CompactorInputSplit split,
                    OutputCollector<NullWritable, NullWritable> nullWritableVOutputCollector,
                    Reporter reporter) throws IOException {
      // This will only get called once, since CompactRecordReader only returns one record,
      // the input split.
      // Based on the split we're passed we go instantiate the real reader and then iterate on it
      // until it finishes.
      @SuppressWarnings("unchecked")//since there is no way to parametrize instance of Class
      AcidInputFormat<WritableComparable, V> aif =
          instantiate(AcidInputFormat.class, jobConf.get(INPUT_FORMAT_CLASS_NAME));
      ValidWriteIdList writeIdList =
          new ValidCompactorWriteIdList(jobConf.get(ValidWriteIdList.VALID_WRITEIDS_KEY));

      boolean isMajor = jobConf.getBoolean(IS_MAJOR, false);
      AcidInputFormat.RawReader<V> reader =
          aif.getRawReader(jobConf, isMajor, split.getBucket(),
                  writeIdList, split.getBaseDir(), split.getDeltaDirs());
      RecordIdentifier identifier = reader.createKey();
      V value = reader.createValue();
      getWriter(reporter, reader.getObjectInspector(), split.getBucket());

      AcidUtils.AcidOperationalProperties acidOperationalProperties
          = AcidUtils.getAcidOperationalProperties(jobConf);

      if (!isMajor && acidOperationalProperties.isSplitUpdate()) {
        // When split-update is enabled for ACID, we initialize a separate deleteEventWriter
        // that is used to write all the delete events (in case of minor compaction only). For major
        // compaction, history is not required to be maintained hence the delete events are processed
        // but not re-written separately.
        getDeleteEventWriter(reporter, reader.getObjectInspector(), split.getBucket());
      }

      while (reader.next(identifier, value)) {
        boolean sawDeleteRecord = reader.isDelete(value);
        if (isMajor && sawDeleteRecord) continue;
        if (sawDeleteRecord && deleteEventWriter != null) {
          // When minor compacting, write delete events to a separate file when split-update is
          // turned on.
          deleteEventWriter.write(value);
          reporter.progress();
        } else {
          writer.write(value);
          reporter.progress();
        }
      }
    }

    @Override
    public void configure(JobConf entries) {
      jobConf = entries;
    }

    @Override
    public void close() throws IOException {
      if (writer != null) {
        writer.close(false);
      }
      if (deleteEventWriter != null) {
        deleteEventWriter.close(false);
      }
    }

    private void getWriter(Reporter reporter, ObjectInspector inspector,
                           int bucket) throws IOException {
      if (writer == null) {
        AcidOutputFormat.Options options = new AcidOutputFormat.Options(jobConf);
        options.inspector(inspector)
            .writingBase(jobConf.getBoolean(IS_MAJOR, false))
            .isCompressed(jobConf.getBoolean(IS_COMPRESSED, false))
            .tableProperties(new StringableMap(jobConf.get(TABLE_PROPS)).toProperties())
            .reporter(reporter)
            .minimumWriteId(jobConf.getLong(MIN_TXN, Long.MAX_VALUE))
            .maximumWriteId(jobConf.getLong(MAX_TXN, Long.MIN_VALUE))
            .bucket(bucket)
            .statementId(-1);//setting statementId == -1 makes compacted delta files use
        //delta_xxxx_yyyy format

        // Instantiate the underlying output format
        @SuppressWarnings("unchecked")//since there is no way to parametrize instance of Class
        AcidOutputFormat<WritableComparable, V> aof =
            instantiate(AcidOutputFormat.class, jobConf.get(OUTPUT_FORMAT_CLASS_NAME));

        Path rootDir = new Path(jobConf.get(TMP_LOCATION));
        cleanupTmpLocationOnTaskRetry(options, rootDir);
        writer = aof.getRawRecordWriter(rootDir, options);
      }
   }

    private void cleanupTmpLocationOnTaskRetry(AcidOutputFormat.Options options, Path rootDir) throws IOException {
      Path tmpLocation = AcidUtils.createFilename(rootDir, options);
      FileSystem fs = tmpLocation.getFileSystem(jobConf);

      if (fs.exists(tmpLocation)) {
        fs.delete(tmpLocation, true);
      }
    }

    private void getDeleteEventWriter(Reporter reporter, ObjectInspector inspector,
        int bucket) throws IOException {
      if (deleteEventWriter == null) {
        AcidOutputFormat.Options options = new AcidOutputFormat.Options(jobConf);
        options.inspector(inspector)
          .writingBase(false)
          .writingDeleteDelta(true)   // this is the option which will make it a delete writer
          .isCompressed(jobConf.getBoolean(IS_COMPRESSED, false))
          .tableProperties(new StringableMap(jobConf.get(TABLE_PROPS)).toProperties())
          .reporter(reporter)
          .minimumWriteId(jobConf.getLong(MIN_TXN, Long.MAX_VALUE))
          .maximumWriteId(jobConf.getLong(MAX_TXN, Long.MIN_VALUE))
          .bucket(bucket)
          .statementId(-1);//setting statementId == -1 makes compacted delta files use
        //delta_xxxx_yyyy format

        // Instantiate the underlying output format
        @SuppressWarnings("unchecked")//since there is no way to parametrize instance of Class
        AcidOutputFormat<WritableComparable, V> aof =
          instantiate(AcidOutputFormat.class, jobConf.get(OUTPUT_FORMAT_CLASS_NAME));

        Path rootDir = new Path(jobConf.get(TMP_LOCATION));
        cleanupTmpLocationOnTaskRetry(options, rootDir);
        deleteEventWriter = aof.getRawRecordWriter(rootDir, options);
      }
    }
  }

  static class StringableList extends ArrayList<Path> {
    StringableList() {

    }

    StringableList(String s) {
      String[] parts = s.split(":", 2);
      // read that many chars
      int numElements = Integer.parseInt(parts[0]);
      s = parts[1];
      for (int i = 0; i < numElements; i++) {
        parts = s.split(":", 2);
        int len = Integer.parseInt(parts[0]);
        String val = parts[1].substring(0, len);
        s = parts[1].substring(len);
        add(new Path(val));
      }
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder();
      buf.append(size());
      buf.append(':');
      if (size() > 0) {
        for (Path p : this) {
          String pStr = p.toString();
          buf.append(pStr.length());
          buf.append(':');
          buf.append(pStr);
        }
      }
      return buf.toString();
    }
  }

  private static <T> T instantiate(Class<T> classType, String classname) throws IOException {
    T t = null;
    try {
      Class c = JavaUtils.loadClass(classname);
      Object o = c.newInstance();
      if (classType.isAssignableFrom(o.getClass())) {
        t = (T)o;
      } else {
        String s = classname + " is not an instance of " + classType.getName();
        LOG.error(s);
        throw new IOException(s);
      }
    } catch (ClassNotFoundException e) {
      LOG.error("Unable to instantiate class, " + StringUtils.stringifyException(e));
      throw new IOException(e);
    } catch (InstantiationException e) {
      LOG.error("Unable to instantiate class, " + StringUtils.stringifyException(e));
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      LOG.error("Unable to instantiate class, " + StringUtils.stringifyException(e));
      throw new IOException(e);
    }
    return t;
  }

  static class CompactorOutputCommitter extends OutputCommitter {

    @Override
    public void setupJob(JobContext jobContext) throws IOException {

    }

    @Override
    public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {

    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
      return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {

    }

    @Override
    public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {

    }

    @Override
    public void commitJob(JobContext context) throws IOException {
      JobConf conf = ShimLoader.getHadoopShims().getJobConf(context);
      Path tmpLocation = new Path(conf.get(TMP_LOCATION));//this contains base_xxx or delta_xxx_yyy
      Path finalLocation = new Path(conf.get(FINAL_LOCATION));
      FileSystem fs = tmpLocation.getFileSystem(conf);
      LOG.debug("Moving contents of " + tmpLocation.toString() + " to " +
          finalLocation.toString());
      if(!fs.exists(tmpLocation)) {
        /**
         * No 'tmpLocation' may happen if job generated created 0 splits, which happens if all
         * input delta and/or base files were empty or had
         * only {@link org.apache.orc.impl.OrcAcidUtils#getSideFile(Path)} files.
         * So make sure the new base/delta is created.
         */
        AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
            .writingBase(conf.getBoolean(IS_MAJOR, false))
            .isCompressed(conf.getBoolean(IS_COMPRESSED, false))
            .minimumWriteId(conf.getLong(MIN_TXN, Long.MAX_VALUE))
            .maximumWriteId(conf.getLong(MAX_TXN, Long.MIN_VALUE))
            .bucket(0)
            .statementId(-1);
        Path newDeltaDir = AcidUtils.createFilename(finalLocation, options).getParent();
        LOG.info(context.getJobID() + ": " + tmpLocation +
            " not found.  Assuming 0 splits.  Creating " + newDeltaDir);
        fs.mkdirs(newDeltaDir);
        createCompactorMarker(conf, newDeltaDir, fs);
        AcidUtils.OrcAcidVersion.writeVersionFile(newDeltaDir, fs);
        return;
      }
      FileStatus[] contents = fs.listStatus(tmpLocation);//expect 1 base or delta dir in this list
      //we have MIN_TXN, MAX_TXN and IS_MAJOR in JobConf so we could figure out exactly what the dir
      //name is that we want to rename; leave it for another day
      // TODO: if we expect one dir why don't we enforce it?
      for (FileStatus fileStatus : contents) {
        Path tmpPath = fileStatus.getPath();
        //newPath is the base/delta dir
        Path newPath = new Path(finalLocation, tmpPath.getName());
        /* rename(A, B) has "interesting" behavior if A and B are directories. If  B doesn't exist,
        *  it does the expected operation and everything that was in A is now in B.  If B exists,
        *  it will make A a child of B.
        *  This issue can happen if the previous MR job succeeded but HMS was unable to persist compaction result.
        *  We will delete the directory B if it exists to avoid the above issue
        */
        if (fs.exists(newPath)) {
          LOG.info(String.format("Final path %s already exists. Deleting the path to avoid redundant base creation", newPath.toString()));
          fs.delete(newPath, true);
        }
        /* Create the markers in the tmp location and rename everything in the end to prevent race condition between
         * marker creation and split read. */
        AcidUtils.OrcAcidVersion.writeVersionFile(tmpPath, fs);
        createCompactorMarker(conf, tmpPath, fs);
        fs.rename(tmpPath, newPath);
      }
      fs.delete(tmpLocation, true);
    }

    private void createCompactorMarker(JobConf conf, Path finalLocation, FileSystem fs)
        throws IOException {
      if(conf.getBoolean(IS_MAJOR, false)) {
        AcidUtils.MetaDataFile.createCompactorMarker(finalLocation, fs);
      }
    }

    @Override
    public void abortJob(JobContext context, int status) throws IOException {
      JobConf conf = ShimLoader.getHadoopShims().getJobConf(context);
      Path tmpLocation = new Path(conf.get(TMP_LOCATION));
      FileSystem fs = tmpLocation.getFileSystem(conf);
      LOG.debug("Removing " + tmpLocation.toString());
      fs.delete(tmpLocation, true);
    }
  }

  /**
   * Note: similar logic to the main committer; however, no ORC versions and stuff like that.
   * @param from The temp directory used for compactor output. Not the actual base/delta.
   * @param to The final directory; basically a SD directory. Not the actual base/delta.
   */
  private void commitMmCompaction(String from, String to, Configuration conf,
      ValidWriteIdList actualWriteIds) throws IOException {
    Path fromPath = new Path(from), toPath = new Path(to);
    FileSystem fs = fromPath.getFileSystem(conf);
    // Assume the high watermark can be used as maximum transaction ID.
    long maxTxn = actualWriteIds.getHighWatermark();
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
      .writingBase(true).isCompressed(false).maximumWriteId(maxTxn).bucket(0).statementId(-1);
    Path newBaseDir = AcidUtils.createFilename(toPath, options).getParent();
    if (!fs.exists(fromPath)) {
      LOG.info(from + " not found.  Assuming 0 splits. Creating " + newBaseDir);
      fs.mkdirs(newBaseDir);
      AcidUtils.MetaDataFile.createCompactorMarker(toPath, fs);
      return;
    }
    LOG.info("Moving contents of " + from + " to " + to);
    FileStatus[] children = fs.listStatus(fromPath);
    if (children.length != 1) {
      throw new IOException("Unexpected files in the source: " + Arrays.toString(children));
    }
    Path tmpPath = children[0].getPath();
    AcidUtils.MetaDataFile.createCompactorMarker(tmpPath, fs);
    fs.rename(tmpPath, newBaseDir);
    fs.delete(fromPath, true);
  }
}
