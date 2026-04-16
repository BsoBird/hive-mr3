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

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.FloatColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.IntColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.BooleanColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ShortColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ByteColumnVector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import org.apache.fory.format.RowEncoder;
import org.apache.fory.format.row.BinaryRow;
import org.apache.fory.format.row.binary.reader.BinaryRowReader;

/**
 * ForyShuffleVectorizedDeserializeRead provides vectorized deserialization for shuffle using Fory's row format.
 * 
 * This class integrates with ReduceRecordSource to deserialize vectorized rows.
 */
public class ForyShuffleVectorizedDeserializeRead {

  private final ForyShuffleSerDe serDe;
  private final RowEncoder<Object> rowEncoder;
  private final BinaryRow binaryRow;
  private final BinaryRowReader rowReader;
  
  private final int numFields;
  private final Object[] fieldValues;
  private Object[] currentRow;
  
  public ForyShuffleVectorizedDeserializeRead(ForyShuffleSerDe serDe) {
    this.serDe = serDe;
    this.rowEncoder = serDe.getRowEncoder();
    this.binaryRow = new BinaryRow(serDe.getColumnTypes().size());
    this.rowReader = new BinaryRowReader(binaryRow);
    this.numFields = serDe.getColumnNames().size();
    this.fieldValues = new Object[numFields];
  }

  public void setBinaryRow(byte[] data, int offset, int length) {
    binaryRow.pointTo(data, offset, length);
  }

  public boolean nextRow() {
    try {
      currentRow = rowEncoder.fromRow(binaryRow);
      return currentRow != null;
    } catch (Exception e) {
      return false;
    }
  }

  public Object[] getCurrentRow() {
    return currentRow;
  }

  public void deserializeToVectorizedBatch(VectorizedRowBatch batch, int rowIndex,
                                          VectorizedRowBatchCtx batchContext) throws HiveException {
    if (currentRow == null) {
      return;
    }
    
    List<ColumnVector> columnVectors = batch.cols;
    for (int i = 0; i < numFields; i++) {
      setColumnVectorFromValue(columnVectors[i], rowIndex, currentRow[i], 
          serDe.getColumnTypes().get(i));
    }
  }

  private void setColumnVectorFromValue(ColumnVector cv, int rowIndex, Object value, TypeInfo typeInfo) {
    if (value == null) {
      cv.isNull[rowIndex] = true;
      return;
    }
    
    cv.isNull[rowIndex] = false;
    
    if (cv instanceof BooleanColumnVector) {
      ((BooleanColumnVector) cv).vector[rowIndex] = (Boolean) value;
    } else if (cv instanceof ByteColumnVector) {
      ((ByteColumnVector) cv).vector[rowIndex] = (Byte) value;
    } else if (cv instanceof ShortColumnVector) {
      ((ShortColumnVector) cv).vector[rowIndex] = (Short) value;
    } else if (cv instanceof IntColumnVector) {
      ((IntColumnVector) cv).vector[rowIndex] = (Integer) value;
    } else if (cv instanceof LongColumnVector) {
      ((LongColumnVector) cv).vector[rowIndex] = (Long) value;
    } else if (cv instanceof FloatColumnVector) {
      ((FloatColumnVector) cv).vector[rowIndex] = (Float) value;
    } else if (cv instanceof DoubleColumnVector) {
      ((DoubleColumnVector) cv).vector[rowIndex] = (Double) value;
    } else if (cv instanceof BytesColumnVector) {
      String str = (String) value;
      byte[] bytes = str.getBytes();
      ((BytesColumnVector) cv).setRef(rowIndex, bytes, 0, bytes.length);
    } else if (cv instanceof TimestampColumnVector) {
      ((TimestampColumnVector) cv).set(rowIndex, (java.sql.Timestamp) value);
    } else if (cv instanceof DecimalColumnVector) {
      ((DecimalColumnVector) cv).vector[rowIndex] = 
          new org.apache.hadoop.hive.common.type.HiveDecimalWritable((java.math.BigDecimal) value);
    } else if (cv instanceof ListColumnVector) {
      ListColumnVector lcv = (ListColumnVector) cv;
      Object[] elements = (Object[]) value;
      int offset = lcv.childCount;
      lcv.offsets[rowIndex] = offset;
      lcv.lengths[rowIndex] = elements.length;
      for (int i = 0; i < elements.length; i++, lcv.childCount++) {
        setColumnVectorFromValue(lcv.child, lcv.childCount, elements[i], typeInfo);
      }
    } else if (cv instanceof MapColumnVector) {
      MapColumnVector mcv = (MapColumnVector) cv;
      Object[] keyValues = (Object[]) value;
      Object[] keys = (Object[]) keyValues[0];
      Object[] vals = (Object[]) keyValues[1];
      int offset = mcv.childCount;
      mcv.offsets[rowIndex] = offset;
      mcv.lengths[rowIndex] = keys.length;
      for (int i = 0; i < keys.length; i++) {
        setColumnVectorFromValue(mcv.keys, mcv.childCount, keys[i], typeInfo);
        setColumnVectorFromValue(mcv.values, mcv.childCount, vals[i], typeInfo);
        mcv.childCount++;
      }
    } else if (cv instanceof StructColumnVector) {
      StructColumnVector scv = (StructColumnVector) cv;
      Object[] fields = (Object[]) value;
      for (int i = 0; i < fields.length; i++) {
        setColumnVectorFromValue(scv.fields[i], rowIndex, fields[i], typeInfo);
      }
    }
  }

  public ForyShuffleSerDe getSerDe() {
    return serDe;
  }
}
