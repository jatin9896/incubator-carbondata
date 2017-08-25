package org.apache.carbondata.presto.readers;

import java.io.IOException;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.DecimalType;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class DecimalSliceStreamReaderTest {

  int RANDOM_INT = 4;
  double RANDOM_DECIMAL = 4.2313213;

  @Test public void readBlockTest() throws IOException {
    DecimalSliceStreamReader decimalSliceStreamReader = new DecimalSliceStreamReader();
    Object[] object = new Object[] { RANDOM_INT };
    decimalSliceStreamReader.setStreamData(object);
    Block block = decimalSliceStreamReader.readBlock(DecimalType.createDecimalType());
    assertNotNull(block);
  }

  @Test public void readBlockTestWithShortDecimal() throws IOException {
    DecimalSliceStreamReader decimalSliceStreamReader = new DecimalSliceStreamReader();
    Object[] object = new Object[] { RANDOM_INT };
    decimalSliceStreamReader.setStreamData(object);
    Block block = decimalSliceStreamReader.readBlock(DecimalType.createDecimalType(16,0));
    assertNotNull(block);
  }

  @Test public void readBlockTestWithLowActualPrecision() throws IOException {
    DecimalSliceStreamReader decimalSliceStreamReader = new DecimalSliceStreamReader();
    Object[] object = new Object[] { RANDOM_DECIMAL };
    decimalSliceStreamReader.setStreamData(object);
    Block block = decimalSliceStreamReader.readBlock(DecimalType.createDecimalType(20,1));
    assertNotNull(block);
  }

}