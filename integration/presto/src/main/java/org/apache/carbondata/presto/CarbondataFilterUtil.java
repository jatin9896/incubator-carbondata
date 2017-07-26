package org.apache.carbondata.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.*;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class CarbondataFilterUtil {
    private static Map<String,Expression> filterMap= new HashMap<>();
    private static DataType Spi2CarbondataTypeMapper(CarbondataColumnHandle carbondataColumnHandle) {
        Type colType = carbondataColumnHandle.getColumnType();
        if (colType == BooleanType.BOOLEAN) return DataType.BOOLEAN;
        else if (colType == SmallintType.SMALLINT) return DataType.SHORT;
        else if (colType == IntegerType.INTEGER) return DataType.INT;
        else if (colType == BigintType.BIGINT) return DataType.LONG;
        else if (colType == DoubleType.DOUBLE) return DataType.DOUBLE;
        else if (colType == VarcharType.VARCHAR) return DataType.STRING;
        else if (colType == DateType.DATE) return DataType.DATE;
        else if (colType == TimestampType.TIMESTAMP) return DataType.TIMESTAMP;
        else if (colType == DecimalType.createDecimalType(carbondataColumnHandle.getPrecision(),
                carbondataColumnHandle.getScale())) return DataType.DECIMAL;
        else return DataType.STRING;
    }

    /**
     * Convert presto-TupleDomain predication into Carbon scan express condition
     *
     * @param originalConstraint presto-TupleDomain
     * @param carbonTable
     * @return
     */
    public static Expression parseFilterExpression(TupleDomain<ColumnHandle> originalConstraint,
                                                   CarbonTable carbonTable) {
        ImmutableList.Builder<Expression> filters = ImmutableList.builder();

        Domain domain = null;

        for (ColumnHandle c : originalConstraint.getDomains().get().keySet()) {

            // Build ColumnExpresstion for Expresstion(Carbondata)
            CarbondataColumnHandle cdch = (CarbondataColumnHandle) c;
            Type type = cdch.getColumnType();

            DataType coltype = Spi2CarbondataTypeMapper(cdch);
            Expression colExpression = new ColumnExpression(cdch.getColumnName(), coltype);

            domain = originalConstraint.getDomains().get().get(c);
            checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

            if (domain.getValues().isNone()) {
            }

            if (domain.getValues().isAll()) {
            }

            List<Object> singleValues = new ArrayList<>();
            List<Expression> disjuncts = new ArrayList<>();
            for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                } else {
                    List<Expression> rangeConjuncts = new ArrayList<>();
                    if (!range.getLow().isLowerUnbounded()) {
                        Object value = ConvertDataByType(range.getLow().getValue(), type);
                        switch (range.getLow().getBound()) {
                            case ABOVE:
                                if (type == TimestampType.TIMESTAMP) {
                                    //todo not now
                                } else {
                                    GreaterThanExpression greater = new GreaterThanExpression(colExpression,
                                            new LiteralExpression(value, coltype));
                                    rangeConjuncts.add(greater);
                                }
                                break;
                            case EXACTLY:
                                GreaterThanEqualToExpression greater =
                                        new GreaterThanEqualToExpression(colExpression,
                                                new LiteralExpression(value, coltype));
                                rangeConjuncts.add(greater);
                                break;
                            case BELOW:
                                throw new IllegalArgumentException("Low marker should never use BELOW bound");
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                        }
                    }
                    if (!range.getHigh().isUpperUnbounded()) {
                        Object value = ConvertDataByType(range.getHigh().getValue(), type);
                        switch (range.getHigh().getBound()) {
                            case ABOVE:
                                throw new IllegalArgumentException("High marker should never use ABOVE bound");
                            case EXACTLY:
                                LessThanEqualToExpression less = new LessThanEqualToExpression(colExpression,
                                        new LiteralExpression(value, coltype));
                                rangeConjuncts.add(less);
                                break;
                            case BELOW:
                                LessThanExpression less2 =
                                        new LessThanExpression(colExpression, new LiteralExpression(value, coltype));
                                rangeConjuncts.add(less2);
                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                    disjuncts.addAll(rangeConjuncts);
                }
            }
            if (singleValues.size() == 1) {
                Expression ex = null;
                if (coltype.equals(DataType.STRING)) {
                    ex = new EqualToExpression(colExpression,
                            new LiteralExpression(((Slice) singleValues.get(0)).toStringUtf8(), coltype));
                } else if (coltype.equals(DataType.TIMESTAMP) || coltype.equals(DataType.DATE)) {
                    Long value = (Long) singleValues.get(0) * 1000;
                    ex = new EqualToExpression(colExpression,
                            new LiteralExpression(value, coltype));
                } else ex = new EqualToExpression(colExpression,
                        new LiteralExpression(singleValues.get(0), coltype));
                filters.add(ex);
            } else if (singleValues.size() > 1) {
                ListExpression candidates = null;
                List<Expression> exs = singleValues.stream().map((a) -> {
                    return new LiteralExpression(ConvertDataByType(a, type), coltype);
                }).collect(Collectors.toList());
                candidates = new ListExpression(exs);

                if (candidates != null) filters.add(new InExpression(colExpression, candidates));
            } else if (disjuncts.size() > 0) {
                if (disjuncts.size() > 1) {
                    Expression finalFilters = new OrExpression(disjuncts.get(0), disjuncts.get(1));
                    if (disjuncts.size() > 2) {
                        for (int i = 2; i < disjuncts.size(); i++) {
                            filters.add(new AndExpression(finalFilters, disjuncts.get(i)));
                        }
                    }
                } else if (disjuncts.size() == 1) filters.add(disjuncts.get(0));
            }
        }

        Expression finalFilters;
        List<Expression> tmp = filters.build();
        if (tmp.size() > 1) {
            finalFilters = new OrExpression(tmp.get(0), tmp.get(1));
            if (tmp.size() > 2) {
                for (int i = 2; i < tmp.size(); i++) {
                    finalFilters = new OrExpression(finalFilters, tmp.get(i));
                }
            }
        } else if (tmp.size() == 1) finalFilters = tmp.get(0);
        else return null;
        return finalFilters;
    }


    private static Object ConvertDataByType(Object rawdata, Type type) {
        if (type.equals(IntegerType.INTEGER)) return new Integer((rawdata.toString()));
        else if (type.equals(BigintType.BIGINT)) return (Long) rawdata;
        else if (type.equals(VarcharType.VARCHAR)) return ((Slice) rawdata).toStringUtf8();
        else if (type.equals(BooleanType.BOOLEAN)) return (Boolean) (rawdata);

        return rawdata;
    }

    public static Expression getFilters(String key) {
        return filterMap.get(key);
    }

    public static void setFilter(String tableName, Expression filter) {
        filterMap.put(tableName,filter);
    }
}
