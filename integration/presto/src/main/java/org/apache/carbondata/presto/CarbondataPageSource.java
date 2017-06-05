/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.presto;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.block.InterleavedBlock;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.ShortArrayBlock;
import com.facebook.presto.spi.block.SliceArrayBlock;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static java.math.RoundingMode.HALF_UP;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * Carbondata Page Source class for custom Carbondata RecordSet Iteration.
 */
public class CarbondataPageSource implements ConnectorPageSource {

  private static final int ROWS_PER_REQUEST = 4096;
  private final RecordCursor cursor;
  private final List<Type> types;
  private final PageBuilder pageBuilder;
  private final char[] buffer = new char[100];
  private boolean closed;

  public CarbondataPageSource(RecordSet recordSet) {
    this(requireNonNull(recordSet, "recordSet is null").getColumnTypes(), recordSet.cursor());
  }

  public CarbondataPageSource(List<Type> types, RecordCursor cursor) {
    this.cursor = requireNonNull(cursor, "cursor is null");
    this.types = unmodifiableList(new ArrayList<>(requireNonNull(types, "types is null")));
    this.pageBuilder = new PageBuilder(this.types);
  }

  public RecordCursor getCursor() {
    return cursor;
  }

  @Override public long getTotalBytes() {
    return cursor.getTotalBytes();
  }

  @Override public long getCompletedBytes() {
    return cursor.getCompletedBytes();
  }

  @Override public long getReadTimeNanos() {
    return cursor.getReadTimeNanos();
  }

  @Override public boolean isFinished() {
    return closed && pageBuilder.isEmpty();
  }

  @Override public Page getNextPage() {
    if (!closed) {
      int i;
      for (i = 0; i < ROWS_PER_REQUEST; i++) {
        if (pageBuilder.isFull()) {
          break;
        }
        if (!cursor.advanceNextPosition()) {
          closed = true;
          break;
        }

        pageBuilder.declarePosition();
        for (int column = 0; column < types.size(); column++) {
          BlockBuilder output = pageBuilder.getBlockBuilder(column);
          if (cursor.isNull(column)) {
            output.appendNull();
          } else {
            Type type = types.get(column);
            Class<?> javaType = type.getJavaType();
            String javaTypeName = javaType.getSimpleName();
            String base = type.getTypeSignature().getBase();
            switch (base) {
              case "varchar":
              case "decimal":
                Slice slice = cursor.getSlice(column);
                writeSlice(slice, type, output);
                break;
              case "row":
              case "array":
                Object val = cursor.getObject(column);
                writeObject(val, output, type);
                break;
              case "boolean":
                type.writeBoolean(output, cursor.getBoolean(column));
                break;
              case "long":
                type.writeLong(output, cursor.getLong(column));
                break;
              case "double":
                type.writeDouble(output, cursor.getDouble(column));
                break;
              default:
                type.writeObject(output, cursor.getObject(column));
            }
          }
        }
      }
    }

    // only return a page if the buffer is full or we are finishing
    if (pageBuilder.isEmpty() || (!closed && !pageBuilder.isFull())) {
      return null;
    }
    Page page = pageBuilder.build();
    pageBuilder.reset();
    return page;
  }

  private void writeSlice(Slice slice, Type type, BlockBuilder output) {
    if (type instanceof DecimalType) {
      if (isShortDecimal(type)) {
        type.writeLong(output, parseLong((DecimalType) type, slice, 0, slice.length()));
      } else {
        type.writeSlice(output, parseSlice((DecimalType) type, slice, 0, slice.length()));
      }
    } else {
      type.writeSlice(output, slice, 0, slice.length());
    }
  }

  private void writeObject(Object val, BlockBuilder output, Type type) {
    boolean[] isNull = checkNull(val);

    if (type.getTypeSignature().getBase().equals("row")) {
      Object[] data = (Object[]) val;
      List<Type> structElemTypes = type.getTypeParameters();
      Block[] dataBlock = new Block[structElemTypes.size()];
      for (int i = 0; i < structElemTypes.size(); i++) {
        dataBlock[i] = getElementBlock(structElemTypes.get(i), data[i]);
      }

      type.writeObject(output, new InterleavedBlock(dataBlock));

    } else {
      Block arrayBlock = getArrayBlock(type, val, isNull);
      type.writeObject(output, arrayBlock);
    }
  }

  private Block getArrayBlock(Type type, Object val, boolean[] isNull) {

    switch (type.getTypeParameters().get(0).getTypeSignature().getBase()) {
      case "smallint":
      case "integer":
        int[] intArray = getIntData((Integer[]) val);
        return new IntArrayBlock(intArray.length, isNull, intArray);
      case "long":
      case "timestamp":
      case "bigint":
        long[] longArray = getLongData((Long[]) val);
        return new LongArrayBlock(longArray.length, isNull, longArray);
      case "varchar":
        Slice[] stringSlices = getStringSlices(val);
        return new SliceArrayBlock(stringSlices.length, stringSlices);
      case "double":
      case "float":
        Double[] data = (Double[]) val;
        long[] doubleLongData = getLongDataForDouble(data);
        return new LongArrayBlock(doubleLongData.length, isNull, doubleLongData);
      case "boolean":
        Slice[] booleanSlices = getBooleanSlices(val);
        return new SliceArrayBlock(booleanSlices.length, booleanSlices);
      case "decimal":
        Slice[] decimalSlices = getDecimalSlices((BigDecimal[]) val);
        long[] bigDecimalLongValues = new long[decimalSlices.length];
        for(int i=0;i <decimalSlices.length; i++) {
          bigDecimalLongValues[i] = parseLong((DecimalType) type.getTypeParameters().get(0), decimalSlices[i], 0, decimalSlices[i].length());
        }
        return new LongArrayBlock(bigDecimalLongValues.length,
            isNull, bigDecimalLongValues);
      default:
        return null;
    }
  }

  private Slice[] getDecimalSlices(BigDecimal[] decimals) {
  Slice[] decimalSlices = new Slice[decimals.length];
    for (int i=0;i <decimals.length; i++) {
      decimalSlices[i] = utf8Slice(Decimals.toString(decimals[i].unscaledValue(), decimals[i].scale()));
  }
  return decimalSlices;
  }

  private Block getElementBlock(Type structElemType, Object data) {
    //Check for row inside a rowtype
    if (structElemType.getTypeSignature().getBase().equals("row")) {
      int nStructElements = structElemType.getTypeParameters().size();
      Block[] structBlocks = new Block[nStructElements];
      Object[] structElements = (Object[]) data;
      for (int i = 0; i < nStructElements; i++) {
        structBlocks[i] =
            getElementBlock(structElemType.getTypeParameters().get(i), structElements[i]);
      }
      int blockSize = structBlocks[0].getPositionCount();
      int[] offsets = new int[blockSize + 1];
      for (int i = 1; i < offsets.length; i++) {
        offsets[i] = i * nStructElements;
      }
      return new ArrayBlock(structBlocks[0].getPositionCount(), new boolean[blockSize], offsets,
          new InterleavedBlock(structBlocks));

    }
    //Check for array inside a rowtype
    else if (structElemType.getTypeSignature().getBase().equals("array")) {
      Object[] structElements = (Object[]) data;
      Type arrayElemType = structElemType.getTypeParameters().get(0);
      boolean[] isNull = checkNull(structElements);
      Block arrayBlock = getArrayBlock(arrayElemType, structElements, isNull);
      int[] offsets = new int[arrayBlock.getPositionCount() + 1];
      for (int i = 1; i < offsets.length; i++) {
        offsets[i] = i * structElements.length;
      }
      return new ArrayBlock(1, new boolean[arrayBlock.getPositionCount()], offsets, arrayBlock);

    } else {

      //Handling of primitive types inside a rowtype
      switch (structElemType.getDisplayName()) {
        case "integer":
          Integer[] intData = new Integer[] { (Integer) data };
          return new IntArrayBlock(intData.length, new boolean[] { checkNullElement(data) },
              getIntData(intData));
        case "smallint":
          Short[] shortData = new Short[] { (Short) data };
          return new ShortArrayBlock(shortData.length, new boolean[] { checkNullElement(data) },
              getShortData(shortData));
        case "varchar":
          Slice slice = utf8Slice((String) data);
          return new SliceArrayBlock(1, new Slice[] { slice });
        case "timestamp":
        case "bigint":
        case "long":
          Long[] longValue = new Long[] { (Long) data };
          return new LongArrayBlock(longValue.length, new boolean[] { checkNullElement(data) },
              getLongData(longValue));
        case "boolean":
          Slice booleanData = utf8Slice(Boolean.toString((Boolean) data));
          return new SliceArrayBlock(1, new Slice[] { booleanData });
        case "float":
        case "double":
          Double[] doubleData = new Double[] { (Double) data };
          return new LongArrayBlock(doubleData.length, new boolean[] { checkNullElement(data) },
              getLongDataForDouble(doubleData));
        default:
          BigDecimal decimalData = (BigDecimal) data;
          Slice decimalSlice =
              utf8Slice(Decimals.toString(decimalData.unscaledValue(), decimalData.scale()));
          long[] bigDecimalLongValues = new long[] {
              parseLong((DecimalType) structElemType, decimalSlice, 0, decimalSlice.length()) };
          return new LongArrayBlock(bigDecimalLongValues.length,
              new boolean[] { checkNullElement(data) }, bigDecimalLongValues);

      }
    }
  }

  private short[] getShortData(Short[] shortData) {
    short[] data = new short[shortData.length];
    for (int i = 0; i < data.length; i++) {
      // insert some dummy data for null values in int column
      data[i] = Objects.isNull(shortData[i]) ? 0 : shortData[i];
    }
    return data;
  }

  private int[] getIntData(Integer[] intData) {
    int[] data = new int[intData.length];
    for (int i = 0; i < data.length; i++) {
      // insert some dummy data for null values in int column
      data[i] = Objects.isNull(intData[i]) ? 0 : intData[i];
    }
    return data;
  }

  private long[] getLongData(Long[] longData) {
    long[] data = new long[longData.length];
    for (int i = 0; i < data.length; i++) {
      // insert some dummy data for null values in long column
      data[i] = Objects.isNull(longData[i]) ? 0L : longData[i];
    }
    return data;
  }

  private long[] getLongDataForDouble(Double[] doubleData) {
    long[] data = new long[doubleData.length];
    for (int i = 0; i < doubleData.length; i++) {
      //insert dummy data for null values in double column
      data[i] = Objects.isNull(doubleData[i]) ? 0L : Double.doubleToLongBits(doubleData[i]);
    }
    return data;
  }

  private long[] getLongDataForDecimal(BigDecimal[] data) {
    long[] longValues = new long[data.length];
    for (int i = 0; i < data.length; i++) {
      // insert some dummy data for null values in decimal column
      longValues[i] = Objects.isNull(data[i]) ? 0L : data[i].longValue();
    }
    return longValues;
  }

  private Slice[] getBooleanSlices(Object val) {
    Boolean[] data = (Boolean[]) val;
    Slice[] booleanSlices = new Slice[data.length];
    for (int i = 0; i < data.length; i++) {
      booleanSlices[i] = utf8Slice(Boolean.toString(data[i]));
    }
    return booleanSlices;
  }

  private Slice[] getStringSlices(Object val) {
    String[] data = (String[]) val;
    Slice[] stringSlices = new Slice[data.length];
    for (int i = 0; i < data.length; i++) {
      stringSlices[i] = utf8Slice(data[i]);
    }
    return stringSlices;
  }

  private boolean[] checkNull(Object val) {
    Object[] arrData = (Object[]) val;
    boolean[] isNull = new boolean[arrData.length];
    int i;
    for (i = 0; i < arrData.length; i++) {
      isNull[i] = checkNullElement(arrData[i]);
    }
    return isNull;
  }

  private boolean checkNullElement(Object val) {
    return Objects.isNull(val);
  }

  @Override public long getSystemMemoryUsage() {
    return cursor.getSystemMemoryUsage() + pageBuilder.getSizeInBytes();
  }

  @Override public void close() throws IOException {
    closed = true;
    cursor.close();
  }

  private long parseLong(DecimalType type, Slice slice, int offset, int length) {
    BigDecimal decimal = parseBigDecimal(type, slice, offset, length);
    return decimal.unscaledValue().longValue();
  }

  private Slice parseSlice(DecimalType type, Slice slice, int offset, int length) {
    BigDecimal decimal = parseBigDecimal(type, slice, offset, length);
    return encodeUnscaledValue(decimal.unscaledValue());
  }

  private BigDecimal parseBigDecimal(DecimalType type, Slice slice, int offset, int length) {
    checkArgument(length < buffer.length);
    for (int i = 0; i < length; i++) {
      buffer[i] = (char) slice.getByte(offset + i);
    }
    BigDecimal decimal = new BigDecimal(buffer, 0, length);
    checkState(decimal.scale() <= type.getScale(),
        "Read decimal value scale larger than column scale");
    decimal = decimal.setScale(type.getScale(), HALF_UP);
    checkState(decimal.precision() <= type.getPrecision(),
        "Read decimal precision larger than column precision");
    return decimal;
  }
}
