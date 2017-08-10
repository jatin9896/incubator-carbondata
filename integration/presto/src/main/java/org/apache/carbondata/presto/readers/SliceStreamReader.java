package org.apache.carbondata.presto.readers;

import java.io.IOException;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;

import static io.airlift.slice.Slices.utf8Slice;

public class SliceStreamReader implements StreamReader {

  private Object[] streamData;

  public SliceStreamReader() {

  }

  public Block readBlock(Type type) throws IOException {
    int batchSize = streamData.length;
    BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), batchSize);
    if (streamData != null) {
      for (int i = 0; i < batchSize; i++) {
        type.writeSlice(builder, utf8Slice(streamData[i].toString()));
      }
    }
    return builder.build();
  }

  public void setStreamData(Object[] streamData) {
    this.streamData = streamData;
  }
}