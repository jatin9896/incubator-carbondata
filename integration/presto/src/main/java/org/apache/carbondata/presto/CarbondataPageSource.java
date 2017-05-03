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
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.SliceArrayBlock;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.ArrayType;
import io.airlift.slice.Slice;
import org.apache.spark.sql.types.Decimal;

import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.Decimals.rescale;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
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
  private boolean closed;
  private final char[] buffer = new char[100];

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
            if (javaType == boolean.class) {
              type.writeBoolean(output, cursor.getBoolean(column));
            } else if (javaType == long.class) {
              type.writeLong(output, cursor.getLong(column));
            } else if (javaType == double.class) {
              type.writeDouble(output, cursor.getDouble(column));
            } else if (javaType == Block.class) {
              Object val = cursor.getObject(column);
              writeObject(val, output, type);
            } else if (javaType == Slice.class) {
              Slice slice = cursor.getSlice(column);
              if (type instanceof DecimalType) {
                if (isShortDecimal(type)) {
                  type.writeLong(output, parseLong((DecimalType) type, slice, 0, slice.length()));
                } else {
                  type.writeSlice(output, parseSlice((DecimalType) type, slice, 0, slice.length()));
                }
              } else {
                type.writeSlice(output, slice, 0, slice.length());
              }
            } else {
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

  private void writeObject(Object val, BlockBuilder output, Type type) {
    Class arrTypeClass = val.getClass().getComponentType();
    /*ArrayType arrType = (ArrayType)type;
    Type elemType = arrType.getElementType();*/
    boolean[] isNull = checkNull(val);
    if (arrTypeClass == Integer.class/*elemType instanceof IntegerType*/) {
      int[] intArray = Arrays.stream((Integer[]) val).mapToInt(Integer::intValue).toArray();
      type.writeObject(output, new IntArrayBlock(intArray.length, isNull, intArray));
    } else if (arrTypeClass == Long.class/*elemType instanceof BigintType*/) {
      long[] longArray = Arrays.stream((Long[]) val).mapToLong(Long::longValue).toArray();
      type.writeObject(output, new LongArrayBlock(longArray.length, isNull, longArray));
    } else if ((arrTypeClass == String.class) && (type.getDisplayName().contains("string"))/*elemType instanceof VarcharType*/) {
      Slice[] stringSlices = getStringSlices(val);
      type.writeObject(output, new SliceArrayBlock(stringSlices.length, stringSlices));
    } else if (arrTypeClass == Double.class || arrTypeClass == Float.class/*elemType instanceof DoubleType*/) {


      /*Slice[] floatSlices = getFloatSlices(val);
      type.writeObject(output, new SliceArrayBlock(floatSlices.length, floatSlices));
    } else if(arrTypeClass == Double.class) {*/

     // DoubleType doubleType = (DoubleType)((ArrayType) type).getElementType();


      Slice[] doubleSlices = getDoubleSlices(val);
      Double[] data = (Double[]) val;

      /*ArrayType arrayType = getArrayType(DOUBLE);
      for(int i=0;i <data.length; i++) {
        arrayType.getElementType().writeDouble(output, data[i]);
      }*/

      type.writeObject(output, new SliceArrayBlock(doubleSlices.length, doubleSlices));


    } else if (arrTypeClass == Boolean.class/*elemType instanceof BooleanType*/) {
      Slice[] booleanSlices = getBooleanSlices(val);
      type.writeObject(output, new SliceArrayBlock(booleanSlices.length, booleanSlices));
    } else /*if (elemType instanceof DecimalType) */{

      Slice[] decimalSlices = getDecimalSlices(val, type);
      Slice[] parsedSlices = getParsedSlicesForDecimal(decimalSlices, type);
      type.writeObject(output, new SliceArrayBlock(parsedSlices.length, parsedSlices));
    }
  }

  private ArrayType getArrayType(Type type) {
    return new ArrayType(type);
  }

  private Slice[] getParsedSlicesForDecimal(Slice[] slices, Type type) {
    /*ArrayType arrType = ((ArrayType) type);
        Type elemType = arrType.getElementType() ;*/
    Slice[] parsedSlices = new Slice[slices.length];
    for(int i=0;i < slices.length ; i ++) {
      parsedSlices[i] = parseSlice(DecimalType.createDecimalType()/*(DecimalType) elemType*/, slices[i], 0, slices[i].length());
    }
    return parsedSlices;
  }

  private Slice[] getDecimalSlices(Object val, Type type) {
    String[] data = (String[]) val;
   /* if(type instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) type;

    DecimalType decType = (DecimalType)arrayType.getElementType();*/
    DecimalType decType = DecimalType.createDecimalType();
    Slice[] decimalSlices = new Slice[data.length];
    for (int i = 0; i < data.length; i++) {
      BigDecimal bigDecimalValue = new BigDecimal(data[i]);
      if (isShortDecimal(type)) {
        decimalSlices[i] = utf8Slice(Decimals.toString(bigDecimalValue.longValue(), decType.getScale()));
      } else {

        if (bigDecimalValue.scale() > decType.getScale()) {
          BigInteger unscaledDecimal =
              rescale(bigDecimalValue.unscaledValue(), bigDecimalValue.scale(), bigDecimalValue.scale());
          Slice decSlice = Decimals.encodeUnscaledValue(unscaledDecimal);
          decimalSlices[i] = utf8Slice(Decimals.toString(decSlice, decType.getScale()));
        } else {
          BigInteger unscaledDecimal =
              rescale(bigDecimalValue.unscaledValue(), bigDecimalValue.scale(), decType.getScale());
          Slice decSlice = Decimals.encodeUnscaledValue(unscaledDecimal);
          decimalSlices[i] = utf8Slice(Decimals.toString(decSlice, decType.getScale()));
        }
      }
    }
    return decimalSlices;
  } /*else {
    return null;
    }
  }*/

  private Slice[] getBooleanSlices(Object val) {
    Boolean[] data = (Boolean[]) val;
    Slice[] booleanSlices = new Slice[data.length];
    for (int i = 0; i < data.length; i++) {
      booleanSlices[i] = utf8Slice(Boolean.toString(data[i]));
    }
    return booleanSlices;
  }

  private Slice[] getDoubleSlices(Object val) {
    Double[] data = (Double[]) val;
    Slice[] doubleSlices = new Slice[data.length];
    for (int i = 0; i < data.length; i++) {
      doubleSlices[i] = utf8Slice(Double.toString(data[i]));
    }
    return doubleSlices;
  }

  private Slice[] getFloatSlices(Object val) {
    Float[] data = (Float[]) val;
    Slice[] floatSlices = new Slice[data.length];
    for (int i = 0; i < data.length; i++) {
      floatSlices[i] = utf8Slice(Float.toString(data[i]));
    }
    return floatSlices;
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
    //TODO: cast in a generic type
    Object[] arrData = (Object[]) val;
    boolean[] isNull = new boolean[arrData.length];
    int i;
    for (i = 0; i < arrData.length; i++) {
      if (Objects.isNull(arrData[i])) {
        isNull[i] = true;
      } else {
        isNull[i] = false;
      }
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
