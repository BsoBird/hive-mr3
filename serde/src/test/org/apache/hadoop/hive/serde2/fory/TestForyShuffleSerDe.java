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

import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit tests for ForyShuffleSerDe and related classes.
 */
public class TestForyShuffleSerDe extends TestCase {

  public TestForyShuffleSerDe(String name) {
    super(name);
  }

  public static Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTest(new TestForyShuffleSerDe("testForyShuffleSerDeInitialization"));
    suite.addTest(new TestForyShuffleSerDe("testForyShuffleSerializeWrite"));
    suite.addTest(new TestForyShuffleSerDe("testForyShuffleDeserializeRead"));
    return suite;
  }

  public void testForyShuffleSerDeInitialization() throws SerDeException {
    Properties props = new Properties();
    props.setProperty("columns", "id,name,value");
    props.setProperty("columns.types", "int:string:double");
    
    ForyShuffleSerDe serDe = new ForyShuffleSerDe();
    serDe.initialize(props, new Properties());
    
    ObjectInspector oi = serDe.getObjectInspector();
    assertTrue("Should be StructObjectInspector", oi instanceof StructObjectInspector);
    
    StructObjectInspector structOI = (StructObjectInspector) oi;
    assertEquals("Should have 3 columns", 3, structOI.getAllStructFieldRefs().size());
  }

  public void testForyShuffleSerializeWrite() throws SerDeException {
    Properties props = new Properties();
    props.setProperty("columns", "id,name,value");
    props.setProperty("columns.types", "int:string:double");
    
    ForyShuffleSerDe serDe = new ForyShuffleSerDe();
    serDe.initialize(props, new Properties());
    
    ForyShuffleSerializeWrite write = new ForyShuffleSerializeWrite(serDe);
    
    write.writeInt(1);
    write.writeString("test".getBytes(), 0, 4);
    write.writeDouble(3.14);
    
    Object[] values = write.getFieldValues();
    assertEquals("Should have 3 fields", 3, values.length);
    assertEquals(Integer.valueOf(1), values[0]);
    assertEquals("test", values[1]);
    assertEquals(Double.valueOf(3.14), values[2]);
  }

  public void testForyShuffleDeserializeRead() throws SerDeException {
    Properties props = new Properties();
    props.setProperty("columns", "id,name,value");
    props.setProperty("columns.types", "int:string:double");
    
    ForyShuffleSerDe serDe = new ForyShuffleSerDe();
    serDe.initialize(props, new Properties());
    
    TypeInfo[] typeInfos = new TypeInfo[] {
        TypeInfoFactory.getPrimitiveTypeInfo(PrimitiveObjectInspector.PrimitiveCategory.INT),
        TypeInfoFactory.getPrimitiveTypeInfo(PrimitiveObjectInspector.PrimitiveCategory.STRING),
        TypeInfoFactory.getPrimitiveTypeInfo(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE)
    };
    
    ForyShuffleDeserializeRead read = new ForyShuffleDeserializeRead(
        typeInfos, false, serDe);
    
    Object[] row = new Object[] { Integer.valueOf(1), "test", Double.valueOf(3.14) };
    read.setRow(row);
    
    assertTrue(read.readNextField());
    assertEquals(1, read.currentInt);
    
    assertTrue(read.readNextField());
    assertNotNull(read.currentBytes);
    
    assertTrue(read.readNextField());
    assertEquals(3.14, read.currentDouble, 0.001);
    
    assertFalse(read.readNextField());
  }

  public static void main(String[] args) {
    junit.textui.TestRunner.run(suite());
  }
}
