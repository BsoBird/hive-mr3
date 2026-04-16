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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;

/**
 * ForyShuffleSerializeWrite implements Hive's SerializeWrite interface using Apache Fory's row format.
 * 
 * This class provides vectorized serialization for shuffle operations.
 */
public class ForyShuffleSerializeWrite implements SerializeWrite {

  private Output output;
  private final ForyShuffleSerDe serDe;
  private final Object[] fieldValues;
  private int currentIndex;

  public ForyShuffleSerializeWrite(ForyShuffleSerDe serDe) {
    this.serDe = serDe;
    this.fieldValues = new Object[serDe.getColumnNames().size()];
  }

  @Override
  public void set(Output output) {
    this.output = output;
  }

  @Override
  public void setAppend(Output output) {
    this.output = output;
  }

  @Override
  public void reset() {
    currentIndex = 0;
  }

  public void reset(int capacity) {
    currentIndex = 0;
  }

  @Override
  public void writeNull() throws IOException {
    fieldValues[currentIndex++] = null;
  }

  @Override
  public void writeBoolean(boolean v) throws IOException {
    fieldValues[currentIndex++] = v;
  }

  @Override
  public void writeByte(byte v) throws IOException {
    fieldValues[currentIndex++] = v;
  }

  @Override
  public void writeShort(short v) throws IOException {
    fieldValues[currentIndex++] = v;
  }

  @Override
  public void writeInt(int v) throws IOException {
    fieldValues[currentIndex++] = v;
  }

  @Override
  public void writeLong(long v) throws IOException {
    fieldValues[currentIndex++] = v;
  }

  @Override
  public void writeFloat(float v) throws IOException {
    fieldValues[currentIndex++] = v;
  }

  @Override
  public void writeDouble(double v) throws IOException {
    fieldValues[currentIndex++] = v;
  }

  @Override
  public void writeString(byte[] v) throws IOException {
    fieldValues[currentIndex++] = new String(v, StandardCharsets.UTF_8);
  }

  @Override
  public void writeString(byte[] v, int start, int length) throws IOException {
    fieldValues[currentIndex++] = new String(v, start, length, StandardCharsets.UTF_8);
  }

  @Override
  public void writeHiveChar(HiveChar hiveChar) throws IOException {
    fieldValues[currentIndex++] = hiveChar.getPaddedValue();
  }

  @Override
  public void writeHiveVarchar(HiveVarchar hiveVarchar) throws IOException {
    fieldValues[currentIndex++] = hiveVarchar.getValue();
  }

  @Override
  public void writeBinary(byte[] v) throws IOException {
    fieldValues[currentIndex++] = v;
  }

  @Override
  public void writeBinary(byte[] v, int start, int length) throws IOException {
    byte[] copy = new byte[length];
    System.arraycopy(v, start, copy, 0, length);
    fieldValues[currentIndex++] = copy;
  }

  @Override
  public void writeDate(Date date) throws IOException {
    fieldValues[currentIndex++] = date;
  }

  @Override
  public void writeDate(int dateAsDays) throws IOException {
    fieldValues[currentIndex++] = Date.of(dateAsDays);
  }

  @Override
  public void writeTimestamp(Timestamp vt) throws IOException {
    fieldValues[currentIndex++] = vt;
  }

  @Override
  public void writeHiveIntervalYearMonth(HiveIntervalYearMonth viyt) throws IOException {
    fieldValues[currentIndex++] = viyt;
  }

  @Override
  public void writeHiveIntervalYearMonth(int totalMonths) throws IOException {
    fieldValues[currentIndex++] = HiveIntervalYearMonth.ofMonth(totalMonths);
  }

  @Override
  public void writeHiveIntervalDayTime(HiveIntervalDayTime vidt) throws IOException {
    fieldValues[currentIndex++] = vidt;
  }

  @Override
  public void writeDecimal64(long decimal64Long, int scale) throws IOException {
    fieldValues[currentIndex++] = HiveDecimal.valueOf(decimal64Long, scale);
  }

  @Override
  public void writeHiveDecimal(HiveDecimal dec, int scale) throws IOException {
    fieldValues[currentIndex++] = dec;
  }

  @Override
  public void writeHiveDecimal(HiveDecimalWritable decWritable, int scale) throws IOException {
    fieldValues[currentIndex++] = decWritable.getHiveDecimal();
  }

  @Override
  public void beginList(List list) throws IOException {
    fieldValues[currentIndex++] = list;
  }

  @Override
  public void separateList() throws IOException {
  }

  @Override
  public void finishList() throws IOException {
  }

  @Override
  public void beginMap(Map<?, ?> map) throws IOException {
    fieldValues[currentIndex++] = map;
  }

  @Override
  public void separateKey() throws IOException {
  }

  @Override
  public void separateKeyValuePair() throws IOException {
  }

  @Override
  public void finishMap() throws IOException {
  }

  @Override
  public void beginStruct(List fieldValues) throws IOException {
    fieldValues[currentIndex++] = fieldValues;
  }

  @Override
  public void separateStruct() throws IOException {
  }

  @Override
  public void finishStruct() throws IOException {
  }

  @Override
  public void beginUnion(int tag) throws IOException {
  }

  @Override
  public void finishUnion() throws IOException {
  }

  public Object[] getFieldValues() {
    return fieldValues;
  }

  public int getNumFields() {
    return fieldValues.length;
  }

  public ForyShuffleSerDe getSerDe() {
    return serDe;
  }
}
