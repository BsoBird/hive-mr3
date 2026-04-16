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

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.serde2.AbstractSerDe;

import org.apache.fory.format.DataType;
import org.apache.fory.format.Encoders;
import org.apache.fory.format.RowEncoder;
import org.apache.fory.format.row.BinaryRow;
import org.apache.fory.format.row.binary.writer.BinaryRowWriter;
import org.apache.fory.format.row.binary.reader.BinaryRowReader;
import org.apache.fory.format.schema.Schema;
import org.apache.fory.format.schema.PrimitiveSchema;
import org.apache.fory.format.schema.ListSchema;
import org.apache.fory.format.schema.MapSchema;
import org.apache.fory.format.schema.StructSchema;

/**
 * ForyShuffleSerDe uses Apache Fory's row format for shuffle serialization.
 * 
 * This SerDe provides:
 * - Zero-copy serialization for improved performance
 * - Cache-friendly binary format with fixed offsets
 * - Cross-language compatibility
 */
@SerDeSpec(schemaProps = {serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES})
public class ForyShuffleSerDe extends AbstractSerDe {

  public static final String CLASS_NAME = ForyShuffleSerDe.class.getName();
  
  private List<String> columnNames;
  private List<TypeInfo> columnTypes;
  private TypeInfo rowTypeInfo;
  private ObjectInspector cachedObjectInspector;
  private RowEncoder<Object> rowEncoder;
  private Schema forySchema;
  private BinaryRow reusableBinaryRow;
  
  private SerDeStats stats;
  private boolean lastOperationSerialize;
  private boolean lastOperationDeserialize;

  @Override
  public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties) throws SerDeException {
    super.initialize(configuration, tableProperties, partitionProperties);

    columnNames = getColumnNames();
    columnTypes = getColumnTypes();
    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    cachedObjectInspector = TypeInfoUtils.getStandardObjectInspectorFromTypeInfo(rowTypeInfo);
    
    forySchema = ForyShuffleUtils.createSchema(columnNames, columnTypes);
    rowEncoder = Encoders.row(forySchema);
    reusableBinaryRow = new BinaryRow(columnTypes.size());
    
    stats = new SerDeStats();
    lastOperationSerialize = false;
    lastOperationDeserialize = false;
    
    LOG.info("ForyShuffleSerDe initialized with {} columns: {}", columnNames.size(), columnNames);
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BytesWritable.class;
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    if (!(blob instanceof BytesWritable)) {
      throw new SerDeException("Expected BytesWritable but got " + blob.getClass().getName());
    }
    
    BytesWritable bw = (BytesWritable) blob;
    byte[] data = bw.getBytes();
    int offset = bw.getOffset();
    int length = bw.getLength();
    
    try {
      // 复用 BinaryRow 对象，避免每次分配
      reusableBinaryRow.pointTo(data, offset, length);
      Object[] row = (Object[]) rowEncoder.fromRow(reusableBinaryRow);
      
      lastOperationDeserialize = true;
      lastOperationSerialize = false;
      stats.incrementDeserializedBytes(length);
      
      return row;
    } catch (Exception e) {
      throw new SerDeException("Failed to deserialize Fory row format", e);
    }
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    try {
      Object[] row;
      
      if (obj instanceof Object[]) {
        row = (Object[]) obj;
      } else if (obj instanceof List) {
        row = ((List<?>) obj).toArray();
      } else {
        // Use ObjectInspector to extract field values
        row = extractFields(obj, objInspector);
      }
      
      BinaryRow binaryRow = rowEncoder.toRow(row);
      int size = binaryRow.getSize();
      
      lastOperationSerialize = true;
      lastOperationDeserialize = false;
      stats.incrementSerializedBytes(size);
      
      // 直接使用 BinaryRow 的内部 buffer，不 copy
      BytesWritable bw = new BytesWritable();
      bw.set(binaryRow.getBytes(), 0, size);
      return bw;
    } catch (Exception e) {
      throw new SerDeException("Failed to serialize to Fory row format", e);
    }
  }
  
  private Object[] extractFields(Object obj, ObjectInspector objInspector) throws SerDeException {
    List<String> columnNames = getColumnNames();
    Object[] row = new Object[columnNames.size()];
    
    if (objInspector.getCategory() == ObjectInspector.Category.STRUCT) {
      StructObjectInspector structOI = (StructObjectInspector) objInspector;
      List<StructField> fields = structOI.getAllStructFieldRefs();
      for (int i = 0; i < fields.size(); i++) {
        try {
          row[i] = structOI.getStructFieldData(obj, fields.get(i));
        } catch (Exception e) {
          row[i] = null;
        }
      }
    }
    
    return row;
  }

  public RowEncoder<Object> getRowEncoder() {
    return rowEncoder;
  }

  public Schema getForySchema() {
    return forySchema;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<TypeInfo> getColumnTypes() {
    return columnTypes;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return stats;
  }
}
