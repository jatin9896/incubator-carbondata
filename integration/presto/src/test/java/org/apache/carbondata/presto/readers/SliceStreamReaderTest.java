package org.apache.carbondata.presto.readers;

import com.facebook.presto.spi.type.*;
import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.IOException;


public class SliceStreamReaderTest extends TestCase{
    SliceStreamReader sliceStreamReader;
    Object[] objects ;
    Type type ;

    @BeforeClass
    public void setUp(){
        sliceStreamReader = new SliceStreamReader();
        objects = new Object[]{'a'};
        sliceStreamReader.setStreamData(objects);
        type = CharType.createCharType(1000L);

    }

    @Test
    public void testReadBlock() throws IOException {
        sliceStreamReader.readBlock(type);
        assertNotNull(sliceStreamReader);
    }
}
