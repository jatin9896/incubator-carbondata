package org.apache.carbondata.presto.readers;

import com.facebook.presto.spi.type.*;
import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;


public class DoubleStreamReaderTest extends TestCase{
    DoubleStreamReader doubleStreamReader;
    Object[] objects ;
    Type type ;

    @BeforeClass
    public void setUp(){
        doubleStreamReader = new DoubleStreamReader();
        objects = new Object[]{(double)1};
        doubleStreamReader.setStreamData(objects);
        type = DoubleType.DOUBLE;
    }

    @Test
    public void testReadBlock() throws IOException {
        doubleStreamReader.readBlock(type);
        assertNotNull(doubleStreamReader);
    }
}
