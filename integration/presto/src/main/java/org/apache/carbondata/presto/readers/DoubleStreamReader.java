package org.apache.carbondata.presto.readers;

import java.io.IOException;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;

public class DoubleStreamReader implements StreamReader {

  private Object[] streamData;

  public DoubleStreamReader() {

  }

  public Block readBlock(Type type) throws IOException {
    int batchSize = streamData.length;
    BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), batchSize);
    if (streamData != null) {
      for (int i = 0; i < batchSize; i++) {
        type.writeDouble(builder, (Double) streamData[i]);
      }
    }
    return builder.build();
  }

  public void setStreamData(Object[] streamData) {
    this.streamData = streamData;
  }
}
