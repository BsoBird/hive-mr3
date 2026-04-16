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

package org.apache.hadoop.hive.ql.exec.vector.keyseries;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

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
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.fory.ForyShuffleVectorizedSerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A Fory-based key series for multiple columns where the keys get serialized using Fory row format.
 * 
 * Optimized to avoid unnecessary allocations.
 */
public class VectorKeySeriesForyMultiSerialized extends VectorKeySeriesSerializedImpl<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(
      VectorKeySeriesForyMultiSerialized.class.getName());

  private final ForyShuffleVectorizedSerializeWrite forySerializeWrite;
  private final int numColumns;
  private final int[] columnNums;
  
  private final Object[] fieldValues;
  private final byte[] reusableKeyBytes;
  private int reusableKeySize;

  private final boolean[] hasAnyNulls;

  public VectorKeySeriesForyMultiSerialized(ForyShuffleVectorizedSerializeWrite forySerializeWrite) {
    super(null);
    this.forySerializeWrite = forySerializeWrite;
    this.numColumns = forySerializeWrite.getSerDe().getColumnTypes().size();
    this.columnNums = new int[numColumns];
    for (int i = 0; i < numColumns; i++) {
      columnNums[i] = i;
    }
    this.fieldValues = new Object[numColumns];
    this.hasAnyNulls = new boolean[VectorizedRowBatch.DEFAULT_SIZE];
    this.reusableKeyBytes = new byte[4096];
  }

  public void init(TypeInfo[] typeInfos, int[] columnMap) throws HiveException {
    // Column mapping is already set to sequential in constructor
    // This method exists for API compatibility
  }

  @Override
  public void processBatch(VectorizedRowBatch batch) throws IOException {

    currentBatchSize = batch.size;
    Preconditions.checkState(currentBatchSize > 0);

    int prevKeyStart = 0;
    int prevKeyLength;
    int currentKeyStart = 0;
    output.reset();

    seriesCount = 0;
    boolean prevKeyIsNull;
    duplicateCounts[0] = 1;
    
    if (batch.selectedInUse) {
      int[] selected = batch.selected;
      int index = selected[0];
      
      serializeRowToFory(batch, index);
      
      if (isAllNulls()) {
        seriesIsAllNull[0] = prevKeyIsNull = true;
        prevKeyLength = 0;
        output.setWritePosition(0);
        nonNullKeyCount = 0;
      } else {
        seriesIsAllNull[0] = prevKeyIsNull = false;
        serializedKeyLengths[0] = currentKeyStart = prevKeyLength = getKeySize();
        hasAnyNulls[0] = hasAnyNullsInRow();
        nonNullKeyCount = 1;
      }

      int keyLength;
      for (int logical = 1; logical < currentBatchSize; logical++) {
        index = selected[logical];
        
        serializeRowToFory(batch, index);
        
        if (isAllNulls()) {
          if (prevKeyIsNull) {
            duplicateCounts[seriesCount]++;
          } else {
            duplicateCounts[++seriesCount] = 1;
            seriesIsAllNull[seriesCount] = prevKeyIsNull = true;
          }
          output.setWritePosition(currentKeyStart);
        } else {
          keyLength = getKeySize() - currentKeyStart;
          if (!prevKeyIsNull && isKeyEqual(prevKeyStart, prevKeyLength, currentKeyStart, keyLength)) {
            duplicateCounts[seriesCount]++;
            output.setWritePosition(currentKeyStart);
          } else {
            duplicateCounts[++seriesCount] = 1;
            seriesIsAllNull[seriesCount] = prevKeyIsNull = false;
            prevKeyStart = currentKeyStart;
            serializedKeyLengths[nonNullKeyCount] = prevKeyLength = keyLength;
            currentKeyStart += keyLength;
            hasAnyNulls[nonNullKeyCount] = hasAnyNullsInRow();
            nonNullKeyCount++;
          }
        }
      }
      seriesCount++;
    } else {
      serializeRowToFory(batch, 0);
      
      if (isAllNulls()) {
        seriesIsAllNull[0] = prevKeyIsNull = true;
        prevKeyLength = 0;
        output.setWritePosition(0);
        nonNullKeyCount = 0;
      } else {
        seriesIsAllNull[0] = prevKeyIsNull = false;
        serializedKeyLengths[0] = currentKeyStart = prevKeyLength = getKeySize();
        hasAnyNulls[0] = hasAnyNullsInRow();
        nonNullKeyCount = 1;
      }

      int keyLength;
      for (int index = 1; index < currentBatchSize; index++) {
        serializeRowToFory(batch, index);
        
        if (isAllNulls()) {
          if (prevKeyIsNull) {
            duplicateCounts[seriesCount]++;
          } else {
            duplicateCounts[++seriesCount] = 1;
            seriesIsAllNull[seriesCount] = prevKeyIsNull = true;
          }
          output.setWritePosition(currentKeyStart);
        } else {
          keyLength = getKeySize() - currentKeyStart;
          if (!prevKeyIsNull && isKeyEqual(prevKeyStart, prevKeyLength, currentKeyStart, keyLength)) {
            duplicateCounts[seriesCount]++;
            output.setWritePosition(currentKeyStart);
          } else {
            duplicateCounts[++seriesCount] = 1;
            seriesIsAllNull[seriesCount] = prevKeyIsNull = false;
            prevKeyStart = currentKeyStart;
            serializedKeyLengths[nonNullKeyCount] = prevKeyLength = keyLength;
            currentKeyStart += keyLength;
            hasAnyNulls[nonNullKeyCount] = hasAnyNullsInRow();
            nonNullKeyCount++;
          }
        }
      }
      seriesCount++;
    }

    computeSerializedHashCodes();
    positionToFirst();
  }

  private void serializeRowToFory(VectorizedRowBatch batch, int rowIndex) {
    output.reset();
    for (int i = 0; i < numColumns; i++) {
      fieldValues[i] = extractValueFromColumnVector(batch.cols[columnNums[i]], rowIndex);
    }
    
    try {
      forySerializeWrite.serializeFromVectorizedBatch(batch, rowIndex);
    } catch (HiveException e) {
      LOG.error("Error serializing row to Fory format", e);
    }
  }

  private int getKeySize() {
    return forySerializeWrite.getSerializedSize();
  }

  private Object extractValueFromColumnVector(ColumnVector cv, int rowIndex) {
    if (cv.isNull[rowIndex]) {
      return null;
    }
    
    if (cv instanceof LongColumnVector) {
      return ((LongColumnVector) cv).vector[rowIndex];
    } else if (cv instanceof DoubleColumnVector) {
      return ((DoubleColumnVector) cv).vector[rowIndex];
    } else if (cv instanceof BytesColumnVector) {
      BytesColumnVector bcv = (BytesColumnVector) cv;
      return new String(bcv.vector[rowIndex], bcv.start[rowIndex], bcv.length[rowIndex], StandardCharsets.UTF_8);
    }
    
    return null;
  }

  private boolean isAllNulls() {
    for (int i = 0; i < numColumns; i++) {
      if (fieldValues[i] != null) {
        return false;
      }
    }
    return true;
  }

  private boolean hasAnyNullsInRow() {
    for (int i = 0; i < numColumns; i++) {
      if (fieldValues[i] == null) {
        return true;
      }
    }
    return false;
  }

  private boolean isKeyEqual(int offset1, int length1, int offset2, int length2) {
    if (length1 != length2) {
      return false;
    }
    byte[] data = forySerializeWrite.getBuffer();
    return StringExpr.equal(data, offset1, length1, data, offset2, length2);
  }

  @Override
  public void setNextNonNullKey(int nonNullKeyPosition) {
    super.setNextNonNullKey(nonNullKeyPosition);
    currentHasAnyNulls = hasAnyNulls[nonNullKeyPosition];
  }
}
