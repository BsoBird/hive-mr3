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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

import org.apache.fory.format.DataType;
import org.apache.fory.format.schema.Field;
import org.apache.fory.format.schema.ListSchema;
import org.apache.fory.format.schema.MapSchema;
import org.apache.fory.format.schema.PrimitiveSchema;
import org.apache.fory.format.schema.Schema;
import org.apache.fory.format.schema.StructSchema;

/**
 * Utility class for creating Fory schemas from Hive TypeInfo.
 */
public class ForyShuffleUtils {

  private ForyShuffleUtils() {}

  public static Schema createSchema(List<String> fieldNames, List<TypeInfo> fieldTypes) {
    if (fieldNames.size() != fieldTypes.size()) {
      throw new IllegalArgumentException("Field names and types must have same size");
    }
    
    Field[] fields = new Field[fieldNames.size()];
    for (int i = 0; i < fieldNames.size(); i++) {
      fields[i] = Field.of(fieldNames.get(i), createSchemaFromTypeInfo(fieldTypes.get(i)));
    }
    
    return StructSchema.of(fields);
  }

  public static Schema createSchemaFromTypeInfo(TypeInfo typeInfo) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        return createPrimitiveSchema((PrimitiveTypeInfo) typeInfo);
      case LIST:
        ListTypeInfo listType = (ListTypeInfo) typeInfo;
        return ListSchema.of(createSchemaFromTypeInfo(listType.getListElementTypeInfo()));
      case MAP:
        MapTypeInfo mapType = (MapTypeInfo) typeInfo;
        return MapSchema.of(
            createSchemaFromTypeInfo(mapType.getMapKeyTypeInfo()),
            createSchemaFromTypeInfo(mapType.getMapValueTypeInfo())
        );
      case STRUCT:
        StructTypeInfo structType = (StructTypeInfo) typeInfo;
        List<String> names = structType.getAllStructFieldNames();
        List<TypeInfo> types = structType.getAllStructFieldTypeInfos();
        Field[] structFields = new Field[names.size()];
        for (int i = 0; i < names.size(); i++) {
          structFields[i] = Field.of(names.get(i), createSchemaFromTypeInfo(types.get(i)));
        }
        return StructSchema.of(structFields);
      case UNION:
        UnionTypeInfo unionType = (UnionTypeInfo) typeInfo;
        List<TypeInfo> unionTypes = unionType.getAllUnionObjectTypeInfos();
        Schema[] unionSchemas = new Schema[unionTypes.size()];
        for (int i = 0; i < unionTypes.size(); i++) {
          unionSchemas[i] = createSchemaFromTypeInfo(unionTypes.get(i));
        }
        return StructSchema.of(unionSchemas);
      default:
        throw new IllegalArgumentException("Unsupported type info: " + typeInfo);
    }
  }

  private static Schema createPrimitiveSchema(PrimitiveTypeInfo typeInfo) {
    PrimitiveObjectInspector.PrimitiveCategory category = typeInfo.getPrimitiveCategory();
    
    switch (category) {
      case BOOLEAN:
        return PrimitiveSchema.of(DataType.BOOL);
      case BYTE:
        return PrimitiveSchema.of(DataType.INT8);
      case SHORT:
        return PrimitiveSchema.of(DataType.INT16);
      case INT:
        return PrimitiveSchema.of(DataType.INT32);
      case LONG:
        return PrimitiveSchema.of(DataType.INT64);
      case FLOAT:
        return PrimitiveSchema.of(DataType.FLOAT32);
      case DOUBLE:
        return PrimitiveSchema.of(DataType.FLOAT64);
      case STRING:
      case CHAR:
      case VARCHAR:
        return PrimitiveSchema.of(DataType.STRING);
      case BINARY:
        return PrimitiveSchema.of(DataType.BINARY);
      case DATE:
        return PrimitiveSchema.of(DataType.DATE);
      case TIMESTAMP:
        return PrimitiveSchema.of(DataType.TIMESTAMP);
      case INTERVAL_YEAR_MONTH:
        return PrimitiveSchema.of(DataType.INTERVAL_MONTHS);
      case INTERVAL_DAY_TIME:
        return PrimitiveSchema.of(DataType.INTERVAL_DAY_TIME);
      case DECIMAL:
        return PrimitiveSchema.of(DataType.DECIMAL);
      default:
        throw new IllegalArgumentException("Unsupported primitive category: " + category);
    }
  }

  public static DataType getDataTypeFromPrimitiveCategory(PrimitiveObjectInspector.PrimitiveCategory category) {
    switch (category) {
      case BOOLEAN: return DataType.BOOL;
      case BYTE: return DataType.INT8;
      case SHORT: return DataType.INT16;
      case INT: return DataType.INT32;
      case LONG: return DataType.INT64;
      case FLOAT: return DataType.FLOAT32;
      case DOUBLE: return DataType.FLOAT64;
      case STRING:
      case CHAR:
      case VARCHAR: return DataType.STRING;
      case BINARY: return DataType.BINARY;
      case DATE: return DataType.DATE;
      case TIMESTAMP: return DataType.TIMESTAMP;
      case INTERVAL_YEAR_MONTH: return DataType.INTERVAL_MONTHS;
      case INTERVAL_DAY_TIME: return DataType.INTERVAL_DAY_TIME;
      case DECIMAL: return DataType.DECIMAL;
      default: throw new IllegalArgumentException("Unsupported category: " + category);
    }
  }
}
