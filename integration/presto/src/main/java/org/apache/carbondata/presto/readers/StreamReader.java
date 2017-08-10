package org.apache.carbondata.presto.readers;

import java.io.IOException;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;


public interface StreamReader {

  Block readBlock(Type type) throws IOException;

  void setStreamData(Object[] data);
}