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

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.SerializableHiveBinaryComparable;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.VectorizationContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import org.apache.fory.format.row.binary.writer.BinaryRowWriter;

/**
 * ForyShuffleFactory creates Fory-based shuffle serializers/deserializers.
 * 
 * This factory provides the integration point between Hive's shuffle system and Fory's row format.
 */
public class ForyShuffleFactory {

  public static final String FORY_SHUFFLE_SERDE_CLASS = "org.apache.hadoop.hive.serde2.fory.ForyShuffleSerDe";

  private ForyShuffleFactory() {}

  /**
   * Creates a SerializeWrite for the given ReduceSinkDesc.
   * 
   * @param conf The ReduceSinkDesc containing serialization configuration
   * @param hconf The Hadoop configuration
   * @param isKey Whether this is for key (true) or value (false) serialization
   * @param columnMap The column mapping
   * @param typeInfos The type information for columns
   * @return A SerializeWrite implementation (Fory or BinarySortable based on config)
   */
  public static SerializeWrite createSerializeWrite(
      ReduceSinkDesc conf, 
      Configuration hconf,
      boolean isKey,
      int[] columnMap,
      TypeInfo[] typeInfos) throws SerDeException {
    
    ForyShuffleConf foryConf = new ForyShuffleConf(hconf);
    
    if (foryConf.isEnabled()) {
      TableDesc tableDesc = isKey ? conf.getKeySerializeInfo() : conf.getValueSerializeInfo();
      Properties props = tableDesc.getProperties();
      ForyShuffleSerDe forySerDe = new ForyShuffleSerDe();
      forySerDe.initialize(hconf, props, new Properties());
      return new ForyShuffleSerializeWrite(forySerDe);
    }
    
    if (isKey) {
      return BinarySortableSerializeWrite.with(
          conf.getKeySerializeInfo().getProperties(), columnMap.length);
    } else {
      return new LazyBinarySerializeWrite(columnMap.length);
    }
  }

  /**
   * Creates a DeserializeRead for the given ReduceSinkDesc.
   * 
   * @param conf The ReduceSinkDesc containing serialization configuration
   * @param hconf The Hadoop configuration
   * @param isKey Whether this is for key (true) or value (false) deserialization
   * @param columnMap The column mapping
   * @param typeInfos The type information for columns
   * @param useExternalBuffer Whether to use external buffer for string conversion
   * @param sortOrders Sort orders for BinarySortable (for keys)
   * @param nullMarkers Null markers for BinarySortable (for keys)
   * @param notNullMarkers Not null markers for BinarySortable (for keys)
   * @return A DeserializeRead implementation
   */
  public static DeserializeRead createDeserializeRead(
      ReduceSinkDesc conf,
      Configuration hconf,
      boolean isKey,
      int[] columnMap,
      TypeInfo[] typeInfos,
      boolean useExternalBuffer,
      boolean[] sortOrders,
      byte[] nullMarkers,
      byte[] notNullMarkers) throws SerDeException {
    
    ForyShuffleConf foryConf = new ForyShuffleConf(hconf);
    
    if (foryConf.isEnabled()) {
      TableDesc tableDesc = isKey ? conf.getKeySerializeInfo() : conf.getValueSerializeInfo();
      Properties props = tableDesc.getProperties();
      ForyShuffleSerDe forySerDe = new ForyShuffleSerDe();
      forySerDe.initialize(hconf, props, new Properties());
      return new ForyShuffleDeserializeRead(typeInfos, useExternalBuffer, forySerDe);
    }
    
    if (isKey) {
      return new BinarySortableDeserializeRead(
          typeInfos, sortOrders, nullMarkers, notNullMarkers, useExternalBuffer);
    } else {
      return new LazyBinaryDeserializeRead(typeInfos, useExternalBuffer);
    }
  }

  /**
   * Creates a ForyShuffleSerDe from table properties.
   */
  public static ForyShuffleSerDe createForyShuffleSerDe(Configuration conf, Properties props) throws SerDeException {
    ForyShuffleSerDe forySerDe = new ForyShuffleSerDe();
    forySerDe.initialize(conf, props, new Properties());
    return forySerDe;
  }

  /**
   * Creates a SerializeWrite for key shuffle.
   */
  public static ForyShuffleSerializeWrite createKeySerializeWrite(
      ReduceSinkDesc conf, Configuration hconf) throws SerDeException {
    TableDesc tableDesc = conf.getKeySerializeInfo();
    Properties props = tableDesc.getProperties();
    ForyShuffleSerDe forySerDe = new ForyShuffleSerDe();
    forySerDe.initialize(hconf, props, new Properties());
    return new ForyShuffleSerializeWrite(forySerDe);
  }

  /**
   * Creates a SerializeWrite for value shuffle.
   */
  public static ForyShuffleSerializeWrite createValueSerializeWrite(
      ReduceSinkDesc conf, Configuration hconf) throws SerDeException {
    TableDesc tableDesc = conf.getValueSerializeInfo();
    Properties props = tableDesc.getProperties();
    ForyShuffleSerDe forySerDe = new ForyShuffleSerDe();
    forySerDe.initialize(hconf, props, new Properties());
    return new ForyShuffleSerializeWrite(forySerDe);
  }

  /**
   * Check if Fory shuffle is enabled in the configuration.
   */
  public static boolean isForyShuffleEnabled(Configuration conf) {
    return ForyShuffleConf.isEnabled(conf);
  }
}
