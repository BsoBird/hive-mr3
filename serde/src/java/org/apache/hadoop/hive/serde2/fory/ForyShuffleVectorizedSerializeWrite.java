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

import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.FloatColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.IntColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.BytesWritable;

import org.apache.fory.format.RowEncoder;
import org.apache.fory.format.row.BinaryRow;

/**
 * ForyShuffleVectorizedSerializeWrite provides vectorized serialization for shuffle using Fory's row format.
 * 
 * Optimized to avoid unnecessary allocations and copies.
 */
public class ForyShuffleVectorizedSerializeWrite {

  private final ForyShuffleSerDe serDe;
  private final RowEncoder<Object> rowEncoder;
  private final int numFields;
  
  private BinaryRow binaryRow;
  private byte[] buffer;
  private int bufferSize;
  
  private final Object[] fieldValues;
  private final BytesWritable reusableBytesWritable;

  public ForyShuffleVectorizedSerializeWrite(ForyShuffleSerDe serDe) {
    this.serDe = serDe;
    this.rowEncoder = serDe.getRowEncoder();
    this.numFields = serDe.getColumnTypes().size();
    this.fieldValues = new Object[numFields];
    this.binaryRow = new BinaryRow(numFields);
    this.buffer = new byte[4096];
    this.bufferSize = 0;
    this.reusableBytesWritable = new BytesWritable();
  }

  public void serializeFromVectorizedBatch(VectorizedRowBatch batch, int rowIndex) throws HiveException {
    ColumnVector[] columnVectors = batch.cols;
    
    for (int i = 0; i < numFields; i++) {
      fieldValues[i] = extractValueFromColumnVector(columnVectors[i], rowIndex, 
          serDe.getColumnTypes().get(i));
    }
    
    try {
      binaryRow = rowEncoder.toRow(fieldValues);
      bufferSize = binaryRow.getSize();
      
      if (buffer.length < bufferSize) {
        buffer = new byte[bufferSize * 2];
      }
      
      System.arraycopy(binaryRow.getBytes(), 0, buffer, 0, bufferSize);
    } catch (Exception e) {
      throw new HiveException("Failed to serialize row to Fory format", e);
    }
  }

  private Object extractValueFromColumnVector(ColumnVector cv, int rowIndex, TypeInfo typeInfo) {
    if (cv.isNull[rowIndex]) {
      return null;
    }
    
    if (cv instanceof org.apache.hadoop.hive.ql.exec.vector.BooleanColumnVector) {
      return ((org.apache.hadoop.hive.ql.exec.vector.BooleanColumnVector) cv).vector[rowIndex];
    } else if (cv instanceof org.apache.hadoop.hive.ql.exec.vector.ByteColumnVector) {
      return ((org.apache.hadoop.hive.ql.exec.vector.ByteColumnVector) cv).vector[rowIndex];
    } else if (cv instanceof org.apache.hadoop.hive.ql.exec.vector.ShortColumnVector) {
      return ((org.apache.hadoop.hive.ql.exec.vector.ShortColumnVector) cv).vector[rowIndex];
    } else if (cv instanceof IntColumnVector) {
      return ((IntColumnVector) cv).vector[rowIndex];
    } else if (cv instanceof LongColumnVector) {
      return ((LongColumnVector) cv).vector[rowIndex];
    } else if (cv instanceof FloatColumnVector) {
      return ((FloatColumnVector) cv).vector[rowIndex];
    } else if (cv instanceof DoubleColumnVector) {
      return ((DoubleColumnVector) cv).vector[rowIndex];
    } else if (cv instanceof BytesColumnVector) {
      BytesColumnVector bcv = (BytesColumnVector) cv;
      return new String(bcv.vector[rowIndex], bcv.start[rowIndex], bcv.length[rowIndex], StandardCharsets.UTF_8);
    } else if (cv instanceof TimestampColumnVector) {
      return ((TimestampColumnVector) cv).asScratchTimestamp(rowIndex);
    } else if (cv instanceof DecimalColumnVector) {
      return ((DecimalColumnVector) cv).vector[rowIndex].getHiveDecimal();
    } else if (cv instanceof ListColumnVector) {
      ListColumnVector lcv = (ListColumnVector) cv;
      int offset = (int) lcv.offsets[rowIndex];
      int length = (int) lcv.lengths[rowIndex];
      Object[] elements = new Object[length];
      TypeInfo elemType = ((ListTypeInfo) typeInfo).getListElementTypeInfo();
      for (int i = 0; i < length; i++) {
        elements[i] = extractValueFromColumnVector(lcv.child, offset + i, elemType);
      }
      return java.util.Arrays.asList(elements);
    } else if (cv instanceof MapColumnVector) {
      MapColumnVector mcv = (MapColumnVector) cv;
      int offset = (int) mcv.offsets[rowIndex];
      int length = (int) mcv.lengths[rowIndex];
      Object[] keys = new Object[length];
      Object[] values = new Object[length];
      MapTypeInfo mapType = (MapTypeInfo) typeInfo;
      for (int i = 0; i < length; i++) {
        keys[i] = extractValueFromColumnVector(mcv.keys, offset + i, mapType.getMapKeyTypeInfo());
        values[i] = extractValueFromColumnVector(mcv.values, offset + i, mapType.getMapValueTypeInfo());
      }
      java.util.HashMap<Object, Object> map = new java.util.HashMap<>(length);
      for (int i = 0; i < length; i++) {
        map.put(keys[i], values[i]);
      }
      return map;
    } else if (cv instanceof StructColumnVector) {
      StructColumnVector scv = (StructColumnVector) cv;
      Object[] fields = new Object[scv.fields.length];
      StructTypeInfo structType = (StructTypeInfo) typeInfo;
      for (int i = 0; i < scv.fields.length; i++) {
        fields[i] = extractValueFromColumnVector(scv.fields[i], rowIndex, structType.getAllStructFieldTypeInfos().get(i));
      }
      return java.util.Arrays.asList(fields);
    }
    
    return null;
  }

  public BytesWritable getSerializedBytes() {
    reusableBytesWritable.set(buffer, 0, bufferSize);
    return reusableBytesWritable;
  }

  public int getSerializedSize() {
    return bufferSize;
  }

  public byte[] getBuffer() {
    return buffer;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public void ensureCapacity(int capacity) {
    if (buffer.length < capacity) {
      byte[] newBuffer = new byte[capacity];
      System.arraycopy(buffer, 0, newBuffer, 0, bufferSize);
      buffer = newBuffer;
    }
  }

  public ForyShuffleSerDe getSerDe() {
    return serDe;
  }
}
