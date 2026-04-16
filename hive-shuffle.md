# Hive MR3 Fory Shuffle Implementation

## 1. Hive Shuffle Architecture Overview

### 1.1 Shuffle Data Flow

```
Map Side:
┌─────────────────────────────────────────────────────────────┐
│  ReduceSinkOperator                                        │
│       │                                                    │
│       ▼                                                    │
│  VectorReduceSinkCommonOperator                            │
│       │                                                    │
│       ├── Key → BinarySortableSerializeWrite              │
│       │         → HiveKey (BytesWritable)                │
│       │                                                    │
│       └── Value → LazyBinarySerializeWrite                │
│                   → BytesWritable                          │
└─────────────────────────────────────────────────────────────┘
                          ↓ Shuffle (Network Transfer)
Reduce Side:
┌─────────────────────────────────────────────────────────────┐
│  ReduceRecordSource                                        │
│       │                                                    │
│       ├── Key → BinarySortableDeserializeRead             │
│       │         → VectorizedRowBatch                      │
│       │                                                    │
│       └── Value → LazyBinaryDeserializeRead               │
│                   → VectorizedRowBatch                      │
└─────────────────────────────────────────────────────────────┘
```

### 1.2 Why Key Must Use BinarySortableSerDe

**BinarySortableSerDe guarantees sort order:**

```java
// If a < b, then serialize(a)'s byte order < serialize(b)'s byte order
// Can directly compare byte[] for sorting, no need to deserialize
int 3 → [03 00 00 00]  // little-endian
int 5 → [05 00 00 00]  // little-endian
// Direct byte[] comparison: 03 < 05 → correctly sorted
```

**Fory BinaryRow does NOT guarantee sort order**, therefore **Key must continue using BinarySortableSerDe**.

### 1.3 Lazy Deserialization in LazyBinary

**LazyBinaryDeserializeRead supports lazy deserialization:**

```java
// Can skip fields when not needed
deserializeField(0);  // Only deserialize field 0
skipField(1);          // Skip field 1
deserializeField(2);  // Only deserialize field 2
```

---

## 2. Fory Row Format Integration Design

### 2.1 Integration Strategy

| Data | Serialization | Deserialization | Reason |
|------|--------------|-----------------|--------|
| **Key** | BinarySortableSerDe | BinarySortableDeserializeRead | Must preserve sort order |
| **Value** | Fory / LazyBinary | Fory (zero-copy) / LazyBinary | Optional, Fory is faster |

### 2.2 Configuration

```sql
-- Enable Fory shuffle (only affects Value)
SET hive.fory.shuffle.enabled=true;
```

### 2.3 Fory Advantages

1. **Zero-Copy Serialization**: Read fields directly from `VectorizedRowBatch` without going through `Object[]`
2. **Zero-Copy Deserialization**: Set directly to `ColumnVector` without creating intermediate objects
3. **Compact Binary**: More efficient than LazyBinary
4. **Cross-Language Support**: Fory BinaryRow can be directly read by Python/C++

---

## 3. Implementation File清单

### 3.1 Serde Module (`serde/src/java/org/apache/hadoop/hive/serde2/fory/`)

| File | Purpose |
|------|---------|
| `ForyShuffleSerDe.java` | Main SerDe class, implements `serialize()`/`deserialize()` |
| `ForyShuffleUtils.java` | Hive TypeInfo → Fory Schema conversion |
| `ForyShuffleSerializeWrite.java` | Implements `SerializeWrite` interface |
| `ForyShuffleDeserializeRead.java` | Implements `DeserializeRead` interface |
| `ForyShuffleVectorizedSerializeWrite.java` | Vectorized batch serialization (zero-copy) |
| `ForyShuffleVectorizedDeserializeRead.java` | Vectorized batch deserialization (zero-copy) |
| `ForyShuffleFactory.java` | Factory class for creating Fory components |
| `ForyShuffleConf.java` | Configuration class |

### 3.2 QL Module Modifications

| File | Changes |
|------|---------|
| `VectorReduceSinkCommonOperator.java` | Added Fory path for Value serialization |
| `VectorReduceSinkEmptyKeyOperator.java` | Added Fory path for Value serialization |
| `ReduceRecordSource.java` | Added Fory zero-copy path for Value deserialization |

### 3.3 Dependencies

**Root pom.xml:**
```xml
<fory.version>0.16.0</fory.version>
```

**serde/pom.xml:**
```xml
<dependency>
  <groupId>org.apache.fory</groupId>
  <artifactId>fory-core</artifactId>
  <version>${fory.version}</version>
</dependency>
```

---

## 4. Core Implementation Details

### 4.1 ForyShuffleVectorizedSerializeWrite

**Zero-Copy Serialization Flow:**

```java
// 1. Extract field values from VectorizedRowBatch
for (int i = 0; i < numFields; i++) {
    fieldValues[i] = extractValueFromColumnVector(batch.cols[i], rowIndex);
}

// 2. Fory RowEncoder serializes to BinaryRow
BinaryRow binaryRow = rowEncoder.toRow(fieldValues);

// 3. Directly use BinaryRow's buffer, no copy
reusableBytesWritable.set(binaryRow.getBytes(), 0, binaryRow.getSize());
```

**Key Optimizations:**
- Reuse `reusableBytesWritable` to avoid `new BytesWritable()` on every call
- Reuse `buffer` to avoid `new byte[]` repeatedly

### 4.2 ForyShuffleVectorizedDeserializeRead

**Zero-Copy Deserialization Flow:**

```java
// 1. BinaryRow pointTo receives data
binaryRow.pointTo(data, offset, length);

// 2. Read directly from BinaryRow, set to ColumnVector
for (int i = 0; i < numFields; i++) {
    if (binaryRow.isNullAt(i)) {
        cv.isNull[rowIndex] = true;
    } else {
        cv.isNull[rowIndex] = false;
        // Direct read, no intermediate Object[]
        ((IntColumnVector) cv).vector[rowIndex] = binaryRow.getInt(i);
    }
}
```

**Key Optimizations:**
- No `Object[]` intermediary
- Direct calls to `binaryRow.getInt()` / `getString()` etc.
- String/Binary uses `setRef()` for zero-copy

### 4.3 Branch Logic in ReduceRecordSource

```java
// Key deserialization: always uses BinarySortable
if (useForyShuffle && foryKeyDeserializeRead != null) {
    // Fory zero-copy (theoretically possible, but Key doesn't support it)
} else {
    keyBinarySortableDeserializeToRow.setBytes(keyBytes, 0, keyLength);
    keyBinarySortableDeserializeToRow.deserialize(batch, 0);
}

// Value deserialization: Fory or LazyBinary
if (useForyShuffle && foryValueDeserializeRead != null) {
    foryValueDeserializeRead.setBinaryRow(valueBytes, 0, valueLength);
    foryValueDeserializeRead.deserializeToVectorizedBatch(batch, rowIdx);
} else {
    valueLazyBinaryDeserializeToRow.setBytes(valueBytes, 0, valueLength);
    valueLazyBinaryDeserializeToRow.deserialize(batch, rowIdx);
}
```

---

## 5. Performance Comparison

### 5.1 Serialization Path

| Step | LazyBinary | Fory |
|------|------------|------|
| Batch → Object[] | ✓ | ✓ |
| Object[] → BinaryRow | N/A | ✓ |
| BinaryRow → buffer | N/A | ✓ direct reference |
| Buffer → BytesWritable | copy | ✓ set() |

### 5.2 Deserialization Path

| Step | LazyBinary | Fory Current | Fory Theoretical |
|------|------------|--------------|------------------|
| BytesWritable → buffer | copy | copy | pointTo() |
| buffer → Object[] | ✓ | ✓ | **not needed** |
| Object[] → ColumnVector | ✓ | ✓ | **direct set** |
| Intermediate object creation | many | many | **none** |

### 5.3 Potential Performance Gains

1. **Reduced object allocation**: `Integer`, `Long`, `String` intermediate objects
2. **Reduced memory copy**: String/Binary uses `setRef()` for zero-copy
3. **Faster serialization**: Fory RowEncoder may be more efficient than LazyBinary

---

## 6. Current Limitations

1. **Key cannot use Fory**: Must keep BinarySortable to support sorting
2. **Union type not fully supported**: Returns null for Union in ForyShuffleVectorizedDeserializeRead
3. **Decimal parsing not optimized**: Still eager parsing, not zero-copy
4. **Needs actual testing**: No real performance test data

---

## 7. Usage

```sql
-- Enable Fory shuffle (only affects Value serialization/deserialization)
SET hive.fory.shuffle.enabled=true;

-- Run queries
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id;
```

---

## 8. Next Steps

1. **Actual performance testing**: Compare LazyBinary vs Fory performance
2. **Arrow integration**: Leverage Fory's Arrow support for columnar analytics
3. **Batch collect**: Reduce `collect()` call count (requires Tez framework support)
4. **Complete Decimal zero-copy**: Parse Fory's decimal encoding
5. **Union type support**: Fully implement Union zero-copy deserialization
