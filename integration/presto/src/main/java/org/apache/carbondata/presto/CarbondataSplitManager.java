/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.presto;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.ListExpression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;
import org.apache.carbondata.presto.impl.CarbonLocalInputSplit;
import org.apache.carbondata.presto.impl.CarbonTableCacheModel;
import org.apache.carbondata.presto.impl.CarbonTableReader;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.carbondata.presto.Types.checkType;

/**
 * Build Carbontable splits
 * filtering irrelevant blocks
 */
public class CarbondataSplitManager implements ConnectorSplitManager {

  private final String connectorId;
  private final CarbonTableReader carbonTableReader;

  @Inject
  public CarbondataSplitManager(CarbondataConnectorId connectorId, CarbonTableReader reader) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.carbonTableReader = requireNonNull(reader, "client is null");
  }

  public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorTableLayoutHandle layout) {
    CarbondataTableLayoutHandle layoutHandle = (CarbondataTableLayoutHandle) layout;
    CarbondataTableHandle tableHandle = layoutHandle.getTable();
    SchemaTableName key = tableHandle.getSchemaTableName();

    // Packaging presto-TupleDomain into CarbondataColumnConstraint, to decouple from presto-spi Module
    List<CarbondataColumnConstraint> rebuildConstraints =
        getColumnConstraints(layoutHandle.getConstraint());

    CarbonTableCacheModel cache = carbonTableReader.getCarbonCache(key);
    Expression filters = CarbondataFilterUtil.parseFilterExpression(layoutHandle.getConstraint(), cache.carbonTable);
    CarbondataFilterUtil.setFilter(layoutHandle.getTable().getSchemaTableName().getTableName(),filters);
    if (cache != null) {
      try {
        List<CarbonLocalInputSplit> splits = carbonTableReader.getInputSplits2(cache, filters);

        ImmutableList.Builder<ConnectorSplit> cSplits = ImmutableList.builder();
        for (CarbonLocalInputSplit split : splits) {
          cSplits.add(new CarbondataSplit(connectorId, tableHandle.getSchemaTableName(),
              layoutHandle.getConstraint(), split, rebuildConstraints));
        }
        return new FixedSplitSource(cSplits.build());
      } catch (Exception ex) {
        System.out.println(ex.toString());
      }
    }
    return null;
  }

  public List<CarbondataColumnConstraint> getColumnConstraints(
      TupleDomain<ColumnHandle> constraint) {
    ImmutableList.Builder<CarbondataColumnConstraint> constraintBuilder = ImmutableList.builder();
    for (TupleDomain.ColumnDomain<ColumnHandle> columnDomain : constraint.getColumnDomains()
        .get()) {
      CarbondataColumnHandle columnHandle =
          checkType(columnDomain.getColumn(), CarbondataColumnHandle.class, "column handle");

      constraintBuilder.add(new CarbondataColumnConstraint(columnHandle.getColumnName(),
          Optional.of(columnDomain.getDomain()), columnHandle.isInvertedIndex()));
    }

    return constraintBuilder.build();
  }
}
