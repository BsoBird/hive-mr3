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
import java.util.List;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;

/**
 * ForyShuffleDeserializeRead implements Hive's DeserializeRead interface using Apache Fory's row format.
 * 
 * This class provides vectorized deserialization for shuffle operations.
 */
public class ForyShuffleDeserializeRead extends DeserializeRead {

  private final ForyShuffleSerDe serDe;
  private Object[] currentRow;
  private int currentIndex;

  public ForyShuffleDeserializeRead(TypeInfo[] typeInfos, DataTypePhysicalVariation[] dataTypePhysicalVariations,
                                   boolean useExternalBuffer, ForyShuffleSerDe serDe) {
    super(typeInfos, dataTypePhysicalVariations, useExternalBuffer);
    this.serDe = serDe;
    this.currentRow = new Object[typeInfos.length];
  }

  public ForyShuffleDeserializeRead(TypeInfo[] typeInfos, boolean useExternalBuffer, ForyShuffleSerDe serDe) {
    super(typeInfos, useExternalBuffer);
    this.serDe = serDe;
    this.currentRow = new Object[typeInfos.length];
  }

  @Override
  public void set(byte[] bytes, int offset, int length) {
    currentIndex = 0;
  }

  public void setRow(Object[] row) {
    this.currentRow = row;
    currentIndex = 0;
  }

  @Override
  public boolean readNextField() throws IOException {
    if (currentIndex >= currentRow.length) {
      return false;
    }
    Object value = currentRow[currentIndex++];
    if (value == null) {
      return false;
    }
    setCurrentValue(value);
    return true;
  }

  @Override
  public void skipNextField() throws IOException {
    currentIndex++;
  }

  @Override
  public boolean isNextComplexMultiValue() throws IOException {
    return false;
  }

  @Override
  public boolean readComplexField() throws IOException {
    return readNextField();
  }

  @Override
  public void finishComplexVariableFieldsType() {
  }

  @Override
  public boolean isEndOfInputReached() {
    return currentIndex >= currentRow.length;
  }

  @Override
  public String getDetailedReadPositionString() {
    return "ForyShuffleDeserializeRead[index=" + currentIndex + "/" + currentRow.length + "]";
  }

  private void setCurrentValue(Object value) {
    if (value == null) {
      return;
    }
    
    TypeInfo typeInfo = typeInfos[currentIndex - 1];
    Category category = typeInfo.getCategory();
    
    if (category != Category.PRIMITIVE) {
      return;
    }
    
    PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
    PrimitiveCategory pc = pti.getPrimitiveCategory();
    
    switch (pc) {
      case BOOLEAN:
        currentBoolean = (Boolean) value;
        break;
      case BYTE:
        currentByte = (Byte) value;
        break;
      case SHORT:
        currentShort = (Short) value;
        break;
      case INT:
        currentInt = (Integer) value;
        break;
      case LONG:
        currentLong = (Long) value;
        break;
      case FLOAT:
        currentFloat = (Float) value;
        break;
      case DOUBLE:
        currentDouble = (Double) value;
        break;
      case STRING:
      case CHAR:
      case VARCHAR:
        String str = (String) value;
        currentBytes = str.getBytes();
        currentBytesStart = 0;
        currentBytesLength = currentBytes.length;
        break;
      case BINARY:
        currentBytes = (byte[]) value;
        currentBytesStart = 0;
        currentBytesLength = currentBytes.length;
        break;
      case DATE:
        currentDateWritable = DateWritableV2.create((Date) value);
        break;
      case TIMESTAMP:
        currentTimestampWritable = new TimestampWritableV2((Timestamp) value);
        break;
      case INTERVAL_YEAR_MONTH:
        currentHiveIntervalYearMonthWritable = new HiveIntervalYearMonthWritable((HiveIntervalYearMonth) value);
        break;
      case INTERVAL_DAY_TIME:
        currentHiveIntervalDayTimeWritable = new HiveIntervalDayTimeWritable((HiveIntervalDayTime) value);
        break;
      case DECIMAL:
        currentHiveDecimalWritable = new HiveDecimalWritable((HiveDecimal) value);
        break;
      default:
        break;
    }
  }

  @Override
  public boolean isReadFieldSupported() {
    return true;
  }

  @Override
  public boolean readField(int fieldIndex) throws IOException {
    if (fieldIndex >= currentRow.length) {
      return false;
    }
    Object value = currentRow[fieldIndex];
    if (value == null) {
      return false;
    }
    
    int prevIndex = currentIndex;
    currentIndex = fieldIndex + 1;
    setCurrentValue(value);
    currentIndex = prevIndex;
    return true;
  }

  public Object[] getCurrentRow() {
    return currentRow;
  }

  public ForyShuffleSerDe getSerDe() {
    return serDe;
  }
}
