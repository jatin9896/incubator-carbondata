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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Strings;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.Decimals.rescale;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;

public class CarbondataRecordCursor implements RecordCursor {

  private static final Logger log = Logger.get(CarbondataRecordCursor.class);
  private final List<CarbondataColumnHandle> columnHandles;

  private List<String> fields;
  private CarbondataSplit split;
  private CarbonIterator<Object[]> rowCursor;
  private CarbonReadSupport<Object[]> readSupport;

  private long totalBytes;
  private long nanoStart;
  private long nanoEnd;

  private Object[] columnData;

  public CarbondataRecordCursor(CarbonReadSupport<Object[]> readSupport,
      CarbonIterator<Object[]> carbonIterator, List<CarbondataColumnHandle> columnHandles,
      CarbondataSplit split) {
    this.rowCursor = carbonIterator;
    this.columnHandles = columnHandles;
    this.readSupport = readSupport;
    this.totalBytes = 0;
  }

  @Override public long getTotalBytes() {
    return totalBytes;
  }

  @Override public long getCompletedBytes() {
    return totalBytes;
  }

  @Override public long getReadTimeNanos() {
    return nanoStart > 0L ? (nanoEnd == 0 ? System.nanoTime() : nanoEnd) - nanoStart : 0L;
  }

  @Override public Type getType(int field) {

    checkArgument(field < columnHandles.size(), "Invalid field index");
    return columnHandles.get(field).getColumnType();
  }

  /**
   * get next Row/Page
   */
  @Override public boolean advanceNextPosition() {

    if (nanoStart == 0) {
      nanoStart = System.nanoTime();
    }

    if (rowCursor.hasNext()) {
      Object[] columns = readSupport.readRow(rowCursor.next());
      columnData = columns.clone();
      fields = new ArrayList<String>();
      if (columns != null && columns.length > 0) {
        for (Object value : columns) {
          if (value != null) {
            fields.add(value.toString());

          } else {
            fields.add(null);
          }
        }
      }
      totalBytes += columns.length;
      return true;
    }
    return false;
  }

  @Override public boolean getBoolean(int field) {
    checkFieldType(field, BOOLEAN);
    return Boolean.parseBoolean(getFieldValue(field));
  }

  @Override public long getLong(int field) {
    String timeStr = getFieldValue(field);
    Type actual = getType(field);
    if (actual instanceof TimestampType) {
      return new Timestamp(Long.parseLong(timeStr)).getTime() / 1000;
    }
    //suppose the
    return Math.round(Double.parseDouble(getFieldValue(field)));
  }

  @Override public double getDouble(int field) {
    checkFieldType(field, DOUBLE);
    return Double.parseDouble(getFieldValue(field));
  }

  @Override public Slice getSlice(int field) {
    Type type = getType(field);
    if (type instanceof DecimalType) {
      DecimalType actual = (DecimalType) type;
      CarbondataColumnHandle carbondataColumnHandle = columnHandles.get(field);
      if (carbondataColumnHandle.getPrecision() > 0) {
        checkFieldType(field, DecimalType.createDecimalType(carbondataColumnHandle.getPrecision(),
            carbondataColumnHandle.getScale()));
      } else {
        checkFieldType(field, DecimalType.createDecimalType());
      }
      String fieldValue = getFieldValue(field);
      BigDecimal bigDecimalValue = new BigDecimal(fieldValue);
      if (isShortDecimal(type)) {
        return utf8Slice(Decimals.toString(bigDecimalValue.unscaledValue(), actual.getScale()));
      } else {
        if (bigDecimalValue.scale() > actual.getScale()) {
          BigInteger unscaledDecimal =
              rescale(bigDecimalValue.unscaledValue(), bigDecimalValue.scale(),
                  bigDecimalValue.scale());
          Slice decimalSlice = Decimals.encodeUnscaledValue(unscaledDecimal);
          return utf8Slice(Decimals.toString(decimalSlice, actual.getScale()));
          //return decimalSlice;
        } else {
          BigInteger unscaledDecimal =
              rescale(bigDecimalValue.unscaledValue(), bigDecimalValue.scale(), actual.getScale());
          Slice decimalSlice = Decimals.encodeUnscaledValue(unscaledDecimal);
          return utf8Slice(Decimals.toString(decimalSlice, actual.getScale()));
          //return decimalSlice;
        }
      }
    } else {
      checkFieldType(field, VARCHAR);
      return utf8Slice(getFieldValue(field));
    }
  }

  @Override public Object getObject(int field) {
    if (columnHandles.get(field).getColumnType().getTypeSignature().getBase().equals("array")) {
      Object arrValues = getData(field);
      return arrValues;
    } else {
      return parseStructData(field);
    }
  }

  private Object parseStructData(int field) {

    List<Type> elemTypes = columnHandles.get(field).getColumnType().getTypeParameters();
    Object[] parsedData = new Object[elemTypes.size()];
    Object[] nestedStructElements;
    for (int i = 0; i < elemTypes.size(); i++) {

      switch (elemTypes.get(i).getTypeSignature().getBase()) {
        case "row":
          GenericInternalRow complexRowData = (GenericInternalRow) columnData[field];
          String complexNestedStructData =
              complexRowData.getStruct(i, elemTypes.get(i).getTypeParameters().size()).toString();
          String[] nestedData =
              complexNestedStructData.replaceAll("\\[", "").replaceAll("\\]", "").split(",");
          nestedStructElements = new Object[nestedData.length];
          for (int j = 0; j < nestedData.length; j++) {
            nestedStructElements[j] =
                getStructElement(nestedData[j], elemTypes.get(i).getTypeParameters().get(j));
          }
          parsedData[i] = nestedStructElements;
          break;
        case "array":
          GenericInternalRow complexArrayData = (GenericInternalRow) columnData[field];
          String complexNestedArrayData =
              Arrays.deepToString(complexArrayData.getArray(field).array());
          String[] arrayElements =
              complexNestedArrayData.replaceAll("\\[", "").replaceAll("\\]", "").split(", ");
          parsedData[i] = getArrayData(elemTypes.get(i).getTypeParameters().get(0), arrayElements);
          break;

        default:
          GenericInternalRow rowData = (GenericInternalRow) columnData[field];
          String nestedStructData = rowData.isNullAt(i) ? "null" : rowData.values()[i].toString();
          parsedData[i] = getStructElement(nestedStructData, elemTypes.get(i));
      }
    }

    return parsedData;
  }

  private Object getStructElement(String elem, Type elemType) {
    if (checkNullValue(elem)) return null;
    else {
      String elementType = elemType.getDisplayName();
      if (elementType.equals("integer")) return Integer.parseInt(elem);
      else if (elementType.equals("boolean")) return Boolean.parseBoolean(elem);
      else if (elementType.equals("bigint") || elementType.equals("long"))
        return Long.parseLong(elem);
      else if (elementType.equals("double")) return Double.parseDouble(elem);
      else if (elementType.equals("float")) return Float.parseFloat(elem);
      else if (elementType.contains("decimal")) return new BigDecimal(elem);
      else if (elementType.equals("timestamp"))
        return new Timestamp(Long.parseLong(elem)).getTime() / 1000;
      else if (elementType.equals("smallint")) return Short.parseShort(elem);
      else return elem;
    }
  }

  private Object getData(int field) {
    String fieldValue = getFieldValue(field);
    String[] data = fieldValue.replaceAll("\\[", "").replaceAll("\\]", "").split(",");

    //For array datatype
    Type arrDataType = columnHandles.get(field).getColumnType().getTypeParameters().get(0);
    return getArrayData(arrDataType, data);
  }

  private Object getArrayData(Type arrDataType, String[] data) {

    switch (arrDataType.getTypeSignature().getBase()) {
      case "boolean":
        Boolean[] boolResults = new Boolean[data.length];
        for (int i = 0; i < boolResults.length; i++) {
          boolResults[i] = checkNullValue(data[i]) ? null : Boolean.parseBoolean(data[i]);
        }
        return boolResults;
      case "long":
      case "bigint":
        Long[] longResults = new Long[data.length];
        for (int i = 0; i < longResults.length; i++) {
          longResults[i] = checkNullValue(data[i]) ? null : Long.parseLong(data[i]);
        }
        return longResults;
      case "integer":
        Integer[] intResults = new Integer[data.length];
        for (int i = 0; i < intResults.length; i++) {
          intResults[i] = checkNullValue(data[i]) ? null : Integer.parseInt(data[i]);
        }
        return intResults;
      case "double":
        Double[] doubleResults = new Double[data.length];
        for (int i = 0; i < doubleResults.length; i++) {
          doubleResults[i] = checkNullValue(data[i]) ? null : Double.parseDouble(data[i]);
        }
        return doubleResults;
      case "float":
        Float[] floatResults = new Float[data.length];
        for (int i = 0; i < floatResults.length; i++) {
          floatResults[i] = checkNullValue(data[i]) ? null : Float.parseFloat(data[i]);
        }
        return floatResults;
      case "decimal":
        BigDecimal[] bigDecimalResults = new BigDecimal[data.length];
        for (int i = 0; i < data.length; i++) {
          bigDecimalResults[i] = checkNullValue(data[i]) ? null : new BigDecimal(data[i]);
        }
        return bigDecimalResults;
      case "timestamp":
        Long[] timestampResults = new Long[data.length];
        for (int i = 0; i < timestampResults.length; i++) {
          timestampResults[i] = checkNullValue(data[i]) ?
              null :
              new Timestamp(Long.parseLong(data[i])).getTime() / 1000;
        }
        return timestampResults;
      default:
        return data;

    }
  }

  private boolean checkNullValue(String val) {
    return val.equals("null");
  }

  @Override public boolean isNull(int field) {
    checkArgument(field < columnHandles.size(), "Invalid field index");
    return Strings.isNullOrEmpty(getFieldValue(field));
  }

  String getFieldValue(int field) {
    checkState(fields != null, "Cursor has not been advanced yet");
    return fields.get(field);
  }

  private void checkFieldType(int field, Type expected) {
    Type actual = getType(field);
    checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field,
        expected, actual);
  }

  @Override public void close() {
    nanoEnd = System.nanoTime();

    //todo  delete cache from readSupport
  }
}
