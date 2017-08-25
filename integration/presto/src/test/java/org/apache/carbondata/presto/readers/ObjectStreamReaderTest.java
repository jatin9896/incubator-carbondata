package org.apache.carbondata.presto.readers;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.*;
import junit.framework.TestCase;
import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.IOException;


public class ObjectStreamReaderTest extends TestCase{
    ObjectStreamReader objectStreamReader;
    Object[] objects ;
    Type type ;

    @BeforeClass
    public void setUp(){
        objectStreamReader = new ObjectStreamReader();
        objects = new Object[]{1};
        objectStreamReader.setStreamData(objects);
        type = BooleanType.BOOLEAN;
    }

    @Test
    public void testReadBlock() throws IOException {
        new MockUp<AbstractType>() {
            @Mock
            public void writeObject(BlockBuilder blockBuilder, Object value){
            }
        };
        objectStreamReader.readBlock(type);
        assertNotNull(objectStreamReader);
    }
}
