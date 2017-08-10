package org.apache.carbondata.presto.readers;

import java.io.IOException;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;

public class BooleanStreamReader implements StreamReader {

  private Object[] streamData;

  BooleanStreamReader() {
  }

  public Block readBlock(Type type) throws IOException {
    int batchSize = streamData.length;
    BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), batchSize);
    if (streamData != null) {
      for (Object aStreamData : streamData) {
        type.writeBoolean(builder, (Boolean) aStreamData);
      }
    }
    return builder.build();
  }

  public void setStreamData(Object[] streamData) {
    this.streamData = streamData;
  }
}