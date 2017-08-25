package org.apache.carbondata.presto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.presto.impl.CarbonLocalInputSplit;
import org.apache.carbondata.presto.impl.CarbonTableReader;
import org.apache.carbondata.presto.scan.executor.impl.ColumnDetailQueryExecutor;
import org.apache.carbondata.presto.scan.result.ColumnBasedResultIterator;
import org.apache.carbondata.presto.scan.result.iterator.AbstractDetailQueryResultIterator;

import com.facebook.presto.hadoop.$internal.io.netty.util.concurrent.DefaultEventExecutorGroup;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.AllOrNoneValueSet;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.concurrent.ExecutorServiceAdapter;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class TestCarbondataPageSourceProvider {

  private static List<ColumnHandle> columnsColHandler;
  private static ConnectorSession connectorSession;
  private static SchemaTableName schemaTableName;
  private static TupleDomain<ColumnHandle> tupleDomain;
  private static CarbonLocalInputSplit carbonLocalInputSplit;
  private static CarbonTableReader reader;
  private static ValueSet valueSet;
  private static Domain domain;
  private static Executor executor;
  private static CarbondataColumnConstraint carbondataColumnConstraint;
  private static ConnectorTransactionHandle transactionHandle;
  private static ConnectorSession session;
  private static ConnectorSplit split;
  private static ColumnHandle columnHandle;
  private static List<CarbondataColumnHandle> carbondataColumnHandleList;
  private static CarbondataColumnHandle carbondataColumnHandle;
  private static CarbonTable carbonTable;
  private static BlockExecutionInfo blockExecutionInfo;
  private static RecordSet recordSet;
  CarbondataConnectorId connectorId = new CarbondataConnectorId("connectorId1");
  private CarbonDictionaryDecodeReadSupport readSupport;
  private CarbondataRecordSetProvider carbondataRecordSetProvider =
      new CarbondataRecordSetProvider(connectorId, reader);
  CarbondataPageSourceProvider carbondataPageSourceProvider =
      new CarbondataPageSourceProvider(carbondataRecordSetProvider);

  private ColumnSchema createColumnarDimensionColumn() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnar(true);
    dimColumn.setColumnName("imei");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataType.STRING);
    dimColumn.setDimensionColumn(true);
    List<Encoding> encodeList =
        new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    encodeList.add(Encoding.DICTIONARY);
    dimColumn.setEncodingList(encodeList);
    dimColumn.setNumberOfChild(0);
    return dimColumn;
  }

  private ColumnSchema createColumnarMeasureColumn() {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnName("id");
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDataType(DataType.INT);
    return dimColumn;
  }

  private TableSchema createTableSchema() {
    TableSchema tableSchema = new TableSchema();
    List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
    columnSchemaList.add(createColumnarMeasureColumn());
    columnSchemaList.add(createColumnarDimensionColumn());
    tableSchema.setListOfColumns(columnSchemaList);
    tableSchema.setTableName("table1");
    return tableSchema;
  }

  private TableInfo createTableInfo(long timeStamp) {
    TableInfo info = new TableInfo();
    info.setDatabaseName("schema1");
    info.setLastUpdatedTime(timeStamp);
    info.setTableUniqueName("schema1_tableName");
    info.setFactTable(createTableSchema());
    info.setStorePath("storePath");
    return info;
  }

  @Test public void createPageSource() {
    new MockUp<CarbondataRecordSetProvider>() {
      @Mock public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle,
          ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {
        carbondataColumnHandle =
            new CarbondataColumnHandle("connectorId", "id", IntegerType.INTEGER, 0, 3, 1, true, 1,
                "int", true, 5, 4);
        carbondataColumnHandleList = new ArrayList<>(Arrays.asList(carbondataColumnHandle));

        carbonTable = CarbonTable.buildFromTableInfo(createTableInfo(1000L));
        connectorSession = new TestingConnectorSession(ImmutableList.of());

        QueryModel queryModel = new QueryModel();
        recordSet = new CarbondataRecordSet(carbonTable, connectorSession, split,
            carbondataColumnHandleList, queryModel);
        return recordSet;
      }
    };

    transactionHandle = new TestingTransactionHandle(UUID.randomUUID());
    session = new TestingConnectorSession(ImmutableList.of());
    schemaTableName = new SchemaTableName("schema", "table");
    tupleDomain = TupleDomain.all();
    List<String> locations = Arrays.asList("str1", "srt2");
    carbonLocalInputSplit =
        new CarbonLocalInputSplit("segmentId", "path", 1, 1, locations, 1, (short) 1);
    valueSet = new AllOrNoneValueSet(IntegerType.INTEGER, true);
    domain = Domain.create(valueSet, false);
    carbondataColumnConstraint =
        new CarbondataColumnConstraint("constraint", Optional.of(domain), false);
    List<CarbondataColumnConstraint> carbondataColumnConstraints =
        Arrays.asList(carbondataColumnConstraint);
    split = new CarbondataSplit("connectorId", schemaTableName, tupleDomain, carbonLocalInputSplit,
        carbondataColumnConstraints);

    columnHandle =
        new CarbondataColumnHandle("connectorId", "id", IntegerType.INTEGER, 0, 3, 1, true, 1,
            "int", true, 5, 4);
    columnsColHandler = new ArrayList<>(1);
    columnsColHandler.add(columnHandle);
    new MockUp<QueryModel>() {
      @Mock public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
        CarbonTableIdentifier carbonTableIdentifier =
            new CarbonTableIdentifier("db", "mytable", "id");
        return new AbsoluteTableIdentifier("defaultpath", carbonTableIdentifier);
      }
    };

    new MockUp<AbstractDetailQueryResultIterator>() {
      @Mock public void intialiseInfos() {
      }

      @Mock protected void initQueryStatiticsModel() {
      }
    };

    new MockUp<ColumnDetailQueryExecutor>() {
      @Mock public CarbonIterator execute(QueryModel queryModel)
          throws QueryExecutionException, IOException {
        blockExecutionInfo = new BlockExecutionInfo();
        List<BlockExecutionInfo> blockExecutionInfoList = Arrays.asList(blockExecutionInfo);
        queryModel = new QueryModel();
        executor = new DefaultEventExecutorGroup(2);
        ExecutorService execService = new ExecutorServiceAdapter(executor);
        ColumnBasedResultIterator columnBasedResultIterator =
            new ColumnBasedResultIterator(blockExecutionInfoList, queryModel, execService);
        return columnBasedResultIterator;
      }
    };
    assertNotNull(carbondataPageSourceProvider
        .createPageSource(transactionHandle, session, split, columnsColHandler));
  }
}