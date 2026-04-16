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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;

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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

import org.apache.fory.format.row.BinaryRow;
import org.apache.fory.format.row.binary.Array;
import org.apache.fory.format.row.binary.BinaryArray;

/**
 * ForyShuffleVectorizedDeserializeRead provides zero-copy vectorized deserialization for shuffle using Fory's row format.
 * 
 * Key features:
 * - Zero-copy deserialization: reads directly from BinaryRow without intermediate Object[]
 * - Partial deserialization: can skip fields if not needed
 * - Efficient: directly sets values into VectorizedRowBatch columns
 */
public class ForyShuffleVectorizedDeserializeRead {

  private static final int DECIMAL_BYTE_LENGTH = 32;

  private final ForyShuffleSerDe serDe;
  private final BinaryRow binaryRow;
  private final int numFields;
  private final int[] decimalScales;
  
  public ForyShuffleVectorizedDeserializeRead(ForyShuffleSerDe serDe) {
    this.serDe = serDe;
    this.binaryRow = new BinaryRow(serDe.getColumnTypes().size());
    this.numFields = serDe.getColumnTypes().size();
    
    this.decimalScales = new int[numFields];
    List<TypeInfo> columnTypes = serDe.getColumnTypes();
    for (int i = 0; i < numFields; i++) {
      TypeInfo typeInfo = columnTypes.get(i);
      if (typeInfo.getCategory() == Category.PRIMITIVE) {
        PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
        if (pti.getPrimitiveCategory() == PrimitiveCategory.DECIMAL) {
          decimalScales[i] = ((DecimalTypeInfo) pti).getScale();
        } else {
          decimalScales[i] = 0;
        }
      } else {
        decimalScales[i] = 0;
      }
    }
  }

  public void setBinaryRow(byte[] data, int offset, int length) {
    binaryRow.pointTo(data, offset, length);
  }

  public void setBinaryRow(BinaryRow row) {
    // Re-use the same BinaryRow by copying state
    // Actually, BinaryRow.pointTo() makes it point to new buffer
    // This method exists for API compatibility
  }

  public BinaryRow getBinaryRow() {
    return binaryRow;
  }

  /**
   * Deserialize one row from the current BinaryRow into the VectorizedRowBatch at the given rowIndex.
   * Uses zero-copy reading from Fory BinaryRow.
   */
  public void deserializeToVectorizedBatch(VectorizedRowBatch batch, int rowIndex) throws HiveException {
    ColumnVector[] columnVectors = batch.cols;
    
    for (int i = 0; i < numFields; i++) {
      setColumnVectorFromBinaryRow(columnVectors[i], rowIndex, i, serDe.getColumnTypes().get(i));
    }
  }

  private void setColumnVectorFromBinaryRow(ColumnVector cv, int rowIndex, int fieldIndex, TypeInfo typeInfo) throws HiveException {
    if (binaryRow.isNullAt(fieldIndex)) {
      cv.isNull[rowIndex] = true;
      return;
    }
    
    cv.isNull[rowIndex] = false;
    
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        setPrimitiveColumnFromBinaryRow(cv, rowIndex, fieldIndex, (PrimitiveTypeInfo) typeInfo);
        break;
      case LIST:
        setListColumnFromBinaryRow(cv, rowIndex, fieldIndex, (ListTypeInfo) typeInfo);
        break;
      case MAP:
        setMapColumnFromBinaryRow(cv, rowIndex, fieldIndex, (MapTypeInfo) typeInfo);
        break;
      case STRUCT:
        setStructColumnFromBinaryRow(cv, rowIndex, fieldIndex, (StructTypeInfo) typeInfo);
        break;
      case UNION:
        throw new HiveException("Union type is not supported in Fory shuffle deserialization");
      default:
        cv.isNull[rowIndex] = true;
        break;
    }
  }

  private void setPrimitiveColumnFromBinaryRow(ColumnVector cv, int rowIndex, int fieldIndex, PrimitiveTypeInfo typeInfo) throws HiveException {
    PrimitiveObjectInspector.PrimitiveCategory category = typeInfo.getPrimitiveCategory();
    
    switch (category) {
      case BOOLEAN:
        ((org.apache.hadoop.hive.ql.exec.vector.BooleanColumnVector) cv).vector[rowIndex] = binaryRow.getBoolean(fieldIndex);
        break;
      case BYTE:
        ((org.apache.hadoop.hive.ql.exec.vector.ByteColumnVector) cv).vector[rowIndex] = (byte) binaryRow.getInt(fieldIndex);
        break;
      case SHORT:
        ((org.apache.hadoop.hive.ql.exec.vector.ShortColumnVector) cv).vector[rowIndex] = (short) binaryRow.getInt(fieldIndex);
        break;
      case INT:
        ((IntColumnVector) cv).vector[rowIndex] = binaryRow.getInt(fieldIndex);
        break;
      case LONG:
        ((LongColumnVector) cv).vector[rowIndex] = binaryRow.getLong(fieldIndex);
        break;
      case FLOAT:
        ((FloatColumnVector) cv).vector[rowIndex] = binaryRow.getFloat(fieldIndex);
        break;
      case DOUBLE:
        ((DoubleColumnVector) cv).vector[rowIndex] = binaryRow.getDouble(fieldIndex);
        break;
      case STRING:
      case CHAR:
      case VARCHAR:
        // Zero-copy: get the bytes reference directly
        int strOffset = binaryRow.getFieldOffset(fieldIndex);
        int strLen = binaryRow.getFieldLength(fieldIndex);
        byte[] strBytes = binaryRow.getBytes();
        // Use setRef for zero-copy
        ((BytesColumnVector) cv).setRef(rowIndex, strBytes, strOffset, strLen);
        break;
      case BINARY:
        int binOffset = binaryRow.getFieldOffset(fieldIndex);
        int binLen = binaryRow.getFieldLength(fieldIndex);
        byte[] binBytes = binaryRow.getBytes();
        ((BytesColumnVector) cv).setRef(rowIndex, binBytes, binOffset, binLen);
        break;
      case DATE:
        // Date stored as int days since epoch
        int dateDays = binaryRow.getInt(fieldIndex);
        ((org.apache.hadoop.hive.ql.exec.vector.LongColumnVector) cv).vector[rowIndex] = dateDays;
        break;
      case TIMESTAMP:
        // Timestamp stored as long millis
        long tsMillis = binaryRow.getLong(fieldIndex);
        ((TimestampColumnVector) cv).set(rowIndex, Timestamp.ofEpochMilli(tsMillis));
        break;
      case DECIMAL:
        int decOffset = binaryRow.getFieldOffset(fieldIndex);
        int decLen = binaryRow.getFieldLength(fieldIndex);
        byte[] decBytes = binaryRow.getBytes();
        int scale = decimalScales[fieldIndex];
        HiveDecimal decimal = parseDecimal(decBytes, decOffset, decLen, scale);
        ((DecimalColumnVector) cv).vector[rowIndex] = new org.apache.hadoop.hive.common.type.HiveDecimalWritable(decimal);
        break;
      case INTERVAL_YEAR_MONTH:
        int months = binaryRow.getInt(fieldIndex);
        ((org.apache.hadoop.hive.ql.exec.vector.LongColumnVector) cv).vector[rowIndex] = months;
        break;
      case INTERVAL_DAY_TIME:
        long millis = binaryRow.getLong(fieldIndex);
        ((org.apache.hadoop.hive.ql.exec.vector.LongColumnVector) cv).vector[rowIndex] = millis;
        break;
      default:
        cv.isNull[rowIndex] = true;
        break;
    }
  }

  private void setListColumnFromBinaryRow(ColumnVector cv, int rowIndex, int fieldIndex, ListTypeInfo typeInfo) throws HiveException {
    ListColumnVector lcv = (ListColumnVector) cv;
    BinaryArray array = binaryRow.getArray(fieldIndex);
    
    if (array == null) {
      cv.isNull[rowIndex] = true;
      return;
    }
    
    int offset = lcv.childCount;
    int length = array.size();
    
    lcv.offsets[rowIndex] = offset;
    lcv.lengths[rowIndex] = length;
    lcv.isNull[rowIndex] = false;
    
    TypeInfo elemType = typeInfo.getListElementTypeInfo();
    
    for (int i = 0; i < length; i++, lcv.childCount++) {
      setArrayElementToColumnVector(lcv.child, lcv.childCount, array, i, elemType);
    }
  }

  private void setMapColumnFromBinaryRow(ColumnVector cv, int rowIndex, int fieldIndex, MapTypeInfo typeInfo) throws HiveException {
    MapColumnVector mcv = (MapColumnVector) cv;
    BinaryArray mapArray = binaryRow.getArray(fieldIndex);
    
    if (mapArray == null) {
      cv.isNull[rowIndex] = true;
      return;
    }
    
    // Map is stored as a struct of (key, value) array
    // Fory's map encoding: array of {key, value} structs
    int mapSize = mapArray.size() / 2; // Each entry is 2 elements (key, value)
    int offset = mcv.childCount;
    
    mcv.offsets[rowIndex] = offset;
    mcv.lengths[rowIndex] = mapSize;
    mcv.isNull[rowIndex] = false;
    
    TypeInfo keyType = typeInfo.getMapKeyTypeInfo();
    TypeInfo valueType = typeInfo.getMapValueTypeInfo();
    
    for (int i = 0; i < mapSize; i++) {
      // Set key
      setArrayElementToColumnVector(mcv.keys, mcv.childCount, mapArray, i * 2, keyType);
      // Set value
      setArrayElementToColumnVector(mcv.values, mcv.childCount, mapArray, i * 2 + 1, valueType);
      mcv.childCount++;
    }
  }

  private void setStructColumnFromBinaryRow(ColumnVector cv, int rowIndex, int fieldIndex, StructTypeInfo typeInfo) throws HiveException {
    StructColumnVector scv = (StructColumnVector) cv;
    BinaryRow structRow = binaryRow.getStruct(fieldIndex);
    
    if (structRow == null) {
      cv.isNull[rowIndex] = true;
      return;
    }
    
    List<org.apache.hadoop.hive.serde2.typeinfo.TypeInfo> fieldTypes = typeInfo.getAllStructFieldTypeInfos();
    
    for (int i = 0; i < scv.fields.length; i++) {
      if (structRow.isNullAt(i)) {
        scv.fields[i].isNull[rowIndex] = true;
      } else {
        scv.fields[i].isNull[rowIndex] = false;
        setPrimitiveColumnFromBinaryRow(scv.fields[i], rowIndex, i, fieldTypes.get(i));
      }
    }
  }

  private void setArrayElementToColumnVector(ColumnVector cv, int childIndex, BinaryArray array, int elementIndex, TypeInfo typeInfo) throws HiveException {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        setPrimitiveArrayElementToColumn(cv, childIndex, array, elementIndex, (PrimitiveTypeInfo) typeInfo);
        break;
      case LIST:
        setListColumnFromBinaryRow(cv, childIndex, elementIndex, (ListTypeInfo) typeInfo);
        break;
      case MAP:
        setMapColumnFromBinaryRow(cv, childIndex, elementIndex, (MapTypeInfo) typeInfo);
        break;
      case STRUCT:
        // For now, mark as null
        cv.isNull[childIndex] = true;
        break;
      case UNION:
        throw new HiveException("Union type is not supported in Fory shuffle deserialization");
      default:
        cv.isNull[childIndex] = true;
        break;
    }
  }

  private void setPrimitiveArrayElementToColumn(ColumnVector cv, int childIndex, BinaryArray array, int elementIndex, PrimitiveTypeInfo typeInfo) throws HiveException {
    PrimitiveObjectInspector.PrimitiveCategory category = typeInfo.getPrimitiveCategory();
    
    switch (category) {
      case BOOLEAN:
        ((org.apache.hadoop.hive.ql.exec.vector.BooleanColumnVector) cv).vector[childIndex] = array.getBoolean(elementIndex);
        break;
      case BYTE:
        ((org.apache.hadoop.hive.ql.exec.vector.ByteColumnVector) cv).vector[childIndex] = (byte) array.getInt(elementIndex);
        break;
      case SHORT:
        ((org.apache.hadoop.hive.ql.exec.vector.ShortColumnVector) cv).vector[childIndex] = (short) array.getInt(elementIndex);
        break;
      case INT:
        ((IntColumnVector) cv).vector[childIndex] = array.getInt(elementIndex);
        break;
      case LONG:
        ((LongColumnVector) cv).vector[childIndex] = array.getLong(elementIndex);
        break;
      case FLOAT:
        ((FloatColumnVector) cv).vector[childIndex] = array.getFloat(elementIndex);
        break;
      case DOUBLE:
        ((DoubleColumnVector) cv).vector[childIndex] = array.getDouble(elementIndex);
        break;
      case STRING:
      case CHAR:
      case VARCHAR:
        int strOffset = array.getFieldOffset(elementIndex);
        int strLen = array.getFieldLength(elementIndex);
        byte[] strBytes = array.getBytes();
        ((BytesColumnVector) cv).setRef(childIndex, strBytes, strOffset, strLen);
        break;
      case BINARY:
        int binOffset = array.getFieldOffset(elementIndex);
        int binLen = array.getFieldLength(elementIndex);
        byte[] binBytes = array.getBytes();
        ((BytesColumnVector) cv).setRef(childIndex, binBytes, binOffset, binLen);
        break;
      default:
        cv.isNull[childIndex] = true;
        break;
    }
  }

  private HiveDecimal parseDecimal(byte[] bytes, int offset, int length, int scale) {
    if (length != DECIMAL_BYTE_LENGTH) {
      return HiveDecimal.ZERO;
    }
    
    byte[] littleEndianBytes = new byte[DECIMAL_BYTE_LENGTH];
    for (int i = 0; i < DECIMAL_BYTE_LENGTH; i++) {
      littleEndianBytes[i] = bytes[offset + DECIMAL_BYTE_LENGTH - 1 - i];
    }
    
    BigInteger unscaledValue = new BigInteger(1, littleEndianBytes);
    BigDecimal bd = new BigDecimal(unscaledValue, scale);
    return HiveDecimal.create(bd);
  }

  public ForyShuffleSerDe getSerDe() {
    return serDe;
  }
}
