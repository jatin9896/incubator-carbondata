package org.apache.carbondata.presto;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.DecimalType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CarbondataUtilTest {
    CarbondataUtil carbondataUtil = new CarbondataUtil();

    @Test
    public void shortDecimalPartitionKeyTest() {
        assertEquals(CarbondataUtil.shortDecimalPartitionKey("1234.567", DecimalType.createDecimalType(7, 3), "column"), 1234567);
    }

    @Test(expected = PrestoException.class)
    public void shortDecimalPartitionKeyTestFoInvalidPrecision() {
        CarbondataUtil.shortDecimalPartitionKey("1234.567", DecimalType.createDecimalType(4, 3), "column");
    }

    @Test(expected = PrestoException.class)
    public void shortDecimalPartitionKeyTestFoInvalidNumber() {
        CarbondataUtil.shortDecimalPartitionKey("number", DecimalType.createDecimalType(4, 3), "column");
    }
}
