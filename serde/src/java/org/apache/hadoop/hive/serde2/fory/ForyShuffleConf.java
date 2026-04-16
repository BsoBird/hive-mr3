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

package org.apache.hadoop.hive.serde2.fory;

import org.apache.hadoop.conf.Configuration;

/**
 * ForyShuffleConf holds configuration settings for Fory row format shuffle.
 */
public class ForyShuffleConf {

  public static final String HIVE_FORY_SHUFFLE_ENABLED = "hive.fory.shuffle.enabled";
  public static final String HIVE_FORY_SHUFFLE_COMPRESS = "hive.fory.shuffle.compress";
  public static final String HIVE_FORY_SHUFFLE_COMPRESSION_CODEC = "hive.fory.shuffle.compression.codec";

  public static final boolean DEFAULT_FORY_SHUFFLE_ENABLED = false;
  public static final boolean DEFAULT_FORY_SHUFFLE_COMPRESS = false;

  private final boolean enabled;
  private final boolean compress;
  private final String compressionCodec;

  public ForyShuffleConf(Configuration conf) {
    this.enabled = conf.getBoolean(HIVE_FORY_SHUFFLE_ENABLED, DEFAULT_FORY_SHUFFLE_ENABLED);
    this.compress = conf.getBoolean(HIVE_FORY_SHUFFLE_COMPRESS, DEFAULT_FORY_SHUFFLE_COMPRESS);
    this.compressionCodec = conf.get(HIVE_FORY_SHUFFLE_COMPRESSION_CODEC, "");
  }

  public ForyShuffleConf(boolean enabled, boolean compress, String compressionCodec) {
    this.enabled = enabled;
    this.compress = compress;
    this.compressionCodec = compressionCodec;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public boolean isCompress() {
    return compress;
  }

  public String getCompressionCodec() {
    return compressionCodec;
  }

  public static void setEnabled(Configuration conf, boolean enabled) {
    conf.setBoolean(HIVE_FORY_SHUFFLE_ENABLED, enabled);
  }

  public static void setCompress(Configuration conf, boolean compress) {
    conf.setBoolean(HIVE_FORY_SHUFFLE_COMPRESS, compress);
  }

  public static void setCompressionCodec(Configuration conf, String codec) {
    conf.set(HIVE_FORY_SHUFFLE_COMPRESSION_CODEC, codec);
  }

  public static boolean isEnabled(Configuration conf) {
    return conf.getBoolean(HIVE_FORY_SHUFFLE_ENABLED, DEFAULT_FORY_SHUFFLE_ENABLED);
  }
}
