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
package org.apache.hadoop.hive.conf;

public class Constants {
  /* Constants for LLAP */
  public static final String LLAP_LOGGER_NAME_QUERY_ROUTING = "query-routing";
  public static final String LLAP_LOGGER_NAME_CONSOLE = "console";
  public static final String LLAP_LOGGER_NAME_RFA = "RFA";
  public static final String LLAP_NUM_BUCKETS = "llap.num.buckets";
  public static final String LLAP_BUCKET_ID = "llap.bucket.id";

  public static final String KAFKA_TOPIC = "kafka.topic";
  public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";

  public static final String JDBC_HIVE_STORAGE_HANDLER_ID =
      "org.apache.hive.storage.jdbc.JdbcStorageHandler";
  public static final String JDBC_CONFIG_PREFIX = "hive.sql";
  public static final String JDBC_TABLE = JDBC_CONFIG_PREFIX + ".table";
  public static final String JDBC_DATABASE_TYPE = JDBC_CONFIG_PREFIX + ".database.type";
  public static final String JDBC_URL = JDBC_CONFIG_PREFIX + ".jdbc.url";
  public static final String JDBC_DRIVER = JDBC_CONFIG_PREFIX + ".jdbc.driver";
  public static final String JDBC_USERNAME = JDBC_CONFIG_PREFIX + ".dbcp.username";
  public static final String JDBC_PASSWORD = JDBC_CONFIG_PREFIX + ".dbcp.password";
  public static final String JDBC_KEYSTORE = JDBC_CONFIG_PREFIX + ".dbcp.password.keystore";
  public static final String JDBC_KEY = JDBC_CONFIG_PREFIX + ".dbcp.password.key";
  public static final String JDBC_QUERY = JDBC_CONFIG_PREFIX + ".query";
  public static final String JDBC_QUERY_FIELD_NAMES = JDBC_CONFIG_PREFIX + ".query.fieldNames";
  public static final String JDBC_QUERY_FIELD_TYPES = JDBC_CONFIG_PREFIX + ".query.fieldTypes";
  public static final String JDBC_SPLIT_QUERY = JDBC_CONFIG_PREFIX + ".query.split";
  public static final String JDBC_PARTITION_COLUMN = JDBC_CONFIG_PREFIX + ".partitionColumn";
  public static final String JDBC_NUM_PARTITIONS = JDBC_CONFIG_PREFIX + ".numPartitions";
  public static final String JDBC_LOW_BOUND = JDBC_CONFIG_PREFIX + ".lowerBound";
  public static final String JDBC_UPPER_BOUND = JDBC_CONFIG_PREFIX + ".upperBound";

  public static final String HIVE_SERVER2_JOB_CREDSTORE_PASSWORD_ENVVAR = "HIVE_JOB_CREDSTORE_PASSWORD";
  public static final String HADOOP_CREDENTIAL_PASSWORD_ENVVAR = "HADOOP_CREDSTORE_PASSWORD";
  public static final String HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG = "hadoop.security.credential.provider.path";

  public static final String MATERIALIZED_VIEW_REWRITING_TIME_WINDOW = "rewriting.time.window";
}
