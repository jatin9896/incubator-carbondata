package org.apache.carbondata.presto.readers;

import com.facebook.presto.spi.type.*;
import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.Test;

public class StreamReadersTest extends TestCase{

    Object[] objects;
    Type type ;
    StreamReaders streamReaders = new StreamReaders();

    @BeforeClass
    public void setUp(){
        objects = new Object[]{1,2};
    }

    @Test
    public void testCreateStreamReaderLongType(){

        type = IntegerType.INTEGER;
        assertEquals(StreamReaders.createStreamReader(type,objects).getClass(),
                IntegerStreamReader.class);

        type = DateType.DATE;
        assertEquals(StreamReaders.createStreamReader(type,objects).getClass(),
                IntegerStreamReader.class);

        type = RealType.REAL;
        assertEquals(StreamReaders.createStreamReader(type,objects).getClass(),
                LongStreamReader.class);

    }

    @Test
    public void testCreateStreamReaderDoubleType(){
        type = DoubleType.DOUBLE;
        assertEquals(StreamReaders.createStreamReader(type,objects).getClass(),
                DoubleStreamReader.class);
    }

    @Test
    public void testCreateStreamReaderSliceType(){
        type = DecimalType.createDecimalType();
        assertEquals(StreamReaders.createStreamReader(type,objects).getClass(),
                DecimalSliceStreamReader.class);

        type = CharType.createCharType(1000L);
        assertEquals(StreamReaders.createStreamReader(type,objects).getClass(),
                SliceStreamReader.class);
    }

    @Test
    public void testCreateStreamReaderOtherType(){
        type = BooleanType.BOOLEAN;
        assertEquals(StreamReaders.createStreamReader(type,objects).getClass(),
                ObjectStreamReader.class);

    }

}
