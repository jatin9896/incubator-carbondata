package org.apache.carbondata.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.carbondata.core.metadata.datatype.DataType.INT;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CarbondataFilterUtilTest {

    @Test
    public void testParseFilterExpression() {
        new MockUp<ColumnSchema>() {
            @Mock
            public DataType getDataType() {
                return INT;
            }
        };
        Type spiType = CarbondataMetadata.CarbondataType2SpiMapper(new ColumnSchema());

        CarbondataColumnHandle cchmock = new CarbondataColumnHandle("", "columnName", spiType, 0, 2, 5, true, 1, "id", false, 0, 0);

        new MockUp<TupleDomain<ColumnHandle>>() {
            @Mock
            public Optional<Map<ColumnHandle, Domain>> getDomains() {
                Map mp = new HashMap<ColumnHandle, Domain>();
                mp.put(cchmock, Domain.all(spiType));
                return Optional.of(mp);
            }
        };
        TupleDomain<ColumnHandle> td = TupleDomain.none();
        Expression expr = CarbondataFilterUtil.parseFilterExpression(td);
        assertNull(expr); // check for empty range
        Long value = new Long(25);

        Domain singleValue = Domain.singleValue(spiType, value);

        new MockUp<TupleDomain<ColumnHandle>>() {
            @Mock
            public Optional<Map<ColumnHandle, Domain>> getDomains() {
                Map mp = new HashMap<ColumnHandle, Domain>();
                mp.put(cchmock, singleValue);
                return Optional.of(mp);
            }
        };
        Expression expr1 = CarbondataFilterUtil.parseFilterExpression(td);
        assertTrue(expr1 instanceof EqualToExpression);

    }

    @Test
    public void testGetFilters() {
        Expression expr=new ColumnExpression("",INT);
        CarbondataFilterUtil.setFilter(2,expr);
        Expression getExpr=CarbondataFilterUtil.getFilters(2);
        assert(getExpr instanceof ColumnExpression);

    }
}
