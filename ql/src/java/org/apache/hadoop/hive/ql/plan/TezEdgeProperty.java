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

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.hive.conf.HiveConf;

public class TezEdgeProperty {

  public enum EdgeType {//todo: HIVE-15549
    SIMPLE_EDGE,//SORT_PARTITION_EDGE
    BROADCAST_EDGE,
    CONTAINS,//used for union (all?)
    CUSTOM_EDGE,//CO_PARTITION_EDGE
    CUSTOM_SIMPLE_EDGE,//PARTITION_EDGE
    ONE_TO_ONE_EDGE,
    XPROD_EDGE
  }

  private HiveConf hiveConf;
  private EdgeType edgeType;
  private int numBuckets;

  private boolean isAutoReduce;
  private boolean isSlowStart = true;
  private int minReducer;
  private int maxReducer;
  private long inputSizePerReducer;
  private float minSrcFraction;
  private float maxSrcFraction;
  private Integer bufferSize;

  private boolean isFixed;  // isFixed == true iff auto parallelism should not be used (for MR3), false by default

  public TezEdgeProperty(HiveConf hiveConf, EdgeType edgeType,
      int buckets) {
    this.hiveConf = hiveConf;
    this.edgeType = edgeType;
    this.numBuckets = buckets;
  }

  public TezEdgeProperty(HiveConf hiveConf, EdgeType edgeType, boolean isAutoReduce,
      boolean isSlowStart, int minReducer, int maxReducer, long bytesPerReducer,
      float minSrcFraction, float maxSrcFraction) {
    this(hiveConf, edgeType, -1);
    setAutoReduce(hiveConf, isAutoReduce, minReducer, maxReducer, bytesPerReducer, minSrcFraction, maxSrcFraction);
    this.isSlowStart = isSlowStart;
  }

  public void setAutoReduce(HiveConf hiveConf, boolean isAutoReduce, int minReducer,
      int maxReducer, long bytesPerReducer, float minSrcFraction, float maxSrcFraction) {
    this.hiveConf = hiveConf;
    this.minReducer = minReducer;
    this.maxReducer = maxReducer;
    this.isAutoReduce = isAutoReduce;
    this.inputSizePerReducer = bytesPerReducer;
    this.minSrcFraction = minSrcFraction;
    this.maxSrcFraction = maxSrcFraction;
  }

  public TezEdgeProperty(EdgeType edgeType) {
    this(null, edgeType, -1);
  }

  public EdgeType getEdgeType() {
    return edgeType;
  }

  public HiveConf getHiveConf () {
    return hiveConf;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public boolean isAutoReduce() {
    return isAutoReduce;
  }

  public void setFixed() {
    this.isFixed = true;
  }

  public boolean isFixed() {
    return this.isFixed;
  }

  public int getMinReducer() {
    return minReducer;
  }

  public int getMaxReducer() {
    return maxReducer;
  }

  public long getInputSizePerReducer() {
    return inputSizePerReducer;
  }

  public boolean isSlowStart() {
    return isSlowStart;
  }

  public float getMinSrcFraction() {
    return minSrcFraction;
  }

  public float getMaxSrcFraction() {
    return maxSrcFraction;
  }

  public void setSlowStart(boolean slowStart) {
    this.isSlowStart = slowStart;
  }

  public void setBufferSize(Integer bufferSize) {
    this.bufferSize = bufferSize;
  }

  public Integer getBufferSize() {
    return bufferSize;
  }

  public void setEdgeType(EdgeType type) {
    this.edgeType = type;
  }

}
