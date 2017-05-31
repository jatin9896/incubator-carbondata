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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.block.InterleavedBlock;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.ShortArrayBlock;
import com.facebook.presto.spi.block.SliceArrayBlock;
import com.facebook.presto.spi.type.DecimalType;
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

            switch (javaTypeName) {
              case "boolean":
                type.writeBoolean(output, cursor.getBoolean(column));
                break;
              case "long":
                type.writeLong(output, cursor.getLong(column));
                break;
              case "double":
                type.writeDouble(output, cursor.getDouble(column));
                break;
              case "Block":
                Object val = cursor.getObject(column);
                writeObject(val, output, type);
                break;
              case "Slice":
                Slice slice = cursor.getSlice(column);
                writeSlice(slice, type, output);
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
    Class arrTypeClass = val.getClass().getComponentType();
    String arrClassName = arrTypeClass.getSimpleName();
    boolean[] isNull = checkNull(val);

    if (type.getDisplayName().startsWith("row")) {
      Object[] data = (Object[]) val;
      List<Type> structElemTypes = type.getTypeParameters();
      Block[] dataBlock = new Block[structElemTypes.size()];
      for (int i = 0; i < structElemTypes.size(); i++) {
        boolean[] isElemNull = new boolean[] {isNull[i]};
        switch (structElemTypes.get(i).getDisplayName()) {
          case "integer":
            Integer[] intData = new Integer[] { (Integer) data[i] };
            dataBlock[i] = new IntArrayBlock(intData.length, isElemNull, getIntData(intData));
            break;
          case "smallint":
            Short[] shortData = new Short[] { (Short) data[i] };
            dataBlock[i] = new ShortArrayBlock(shortData.length, isElemNull, getShortData(shortData));
            break;
          case "varchar":
            Slice slice = utf8Slice((String) data[i]);
            dataBlock[i] = new SliceArrayBlock(1, new Slice[] { slice });
            break;
          case "timestamp":
          case "bigint":
          case "long":
            Long[] longValue = new Long[] { (Long) data[i] };
            dataBlock[i] = new LongArrayBlock(longValue.length, isElemNull, getLongData(longValue));
            break;
          case "boolean":
            Slice booleanData = utf8Slice(Boolean.toString((Boolean) data[i]));
            dataBlock[i] = new SliceArrayBlock(1, new Slice[] { booleanData });
            break;
          case "float":
          case "double":
            Double[] doubleData = new Double[] { (Double) data[i] };
            dataBlock[i] =
                new LongArrayBlock(doubleData.length, isElemNull, getLongDataForDouble(doubleData));
            break;
          default:
            BigDecimal[] longForDecimal = new BigDecimal[] { (BigDecimal) data[i] };
            dataBlock[i] = new LongArrayBlock(longForDecimal.length, isElemNull,
                getLongDataForDecimal(longForDecimal));
        }
      }

      type.writeObject(output, new InterleavedBlock(dataBlock));

    } else {
      switch (arrClassName) {
        case "Integer":
          int[] intArray = getIntData((Integer[]) val);
          type.writeObject(output, new IntArrayBlock(intArray.length, isNull, intArray));
          break;
        case "Long":
          long[] longArray = getLongData((Long[]) val);
          type.writeObject(output, new LongArrayBlock(longArray.length, isNull, longArray));
          break;
        case "String":
          Slice[] stringSlices = getStringSlices(val);
          type.writeObject(output, new SliceArrayBlock(stringSlices.length, stringSlices));
          break;
        case "Double":
        case "Float":
          Double[] data = (Double[]) val;
          long[] doubleLongData = getLongDataForDouble(data);
          type.writeObject(output,
              new LongArrayBlock(doubleLongData.length, isNull, doubleLongData));
          break;
        case "Boolean":
          Slice[] booleanSlices = getBooleanSlices(val);
          type.writeObject(output, new SliceArrayBlock(booleanSlices.length, booleanSlices));
          break;
        case "":
        default:
          long[] longDecimalValues = getLongDataForDecimal((BigDecimal[]) val);
          type.writeObject(output,
              new LongArrayBlock(longDecimalValues.length, isNull, longDecimalValues));
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
      isNull[i] = Objects.isNull(arrData[i]);
    }
    return isNull;
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
