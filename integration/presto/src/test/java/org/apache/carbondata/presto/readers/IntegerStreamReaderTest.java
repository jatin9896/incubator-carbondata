package org.apache.carbondata.presto.readers;

import java.io.IOException;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.IntegerType;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class IntegerStreamReaderTest {

  int RANDOM_DATA = 4;

  @Test public void readBlockTest() throws IOException {
    IntegerStreamReader integerStreamReader = new IntegerStreamReader();
    Object[] object = new Object[] { RANDOM_DATA };
    integerStreamReader.setStreamData(object);
    Block block = integerStreamReader.readBlock(IntegerType.INTEGER);
    assertNotNull(block);
  }

}
