package org.apache.carbondata.presto;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.BatchResult;
import org.apache.carbondata.presto.impl.CarbonLocalInputSplit;
import org.apache.carbondata.presto.processor.CarbonDataBlockIterator;
import org.apache.carbondata.presto.processor.impl.ColumnBasedResultCollector;
import org.apache.carbondata.presto.processor.impl.DataBlockIteratorImpl;
import org.apache.carbondata.presto.scan.executor.impl.ColumnDetailQueryExecutor;
import org.apache.carbondata.presto.scan.result.ColumnBasedResultIterator;
import org.apache.carbondata.presto.scan.result.iterator.AbstractDetailQueryResultIterator;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

public class CarbonDataPageSourceTest {

  private static TableInfo tableInfo;
  private static CarbonTable carbonTable;
  private static SchemaTableName schemaTable;
  private static TupleDomain<ColumnHandle> domain;
  private static CarbonLocalInputSplit localSplits;
  private static QueryModel queryModel;
  private static CarbondataSplit split;
  private static CarbondataRecordSet carbondataRecordSet;
  private static CarbondataPageSource carbonPage;
  private static BlockExecutionInfo blockExecutionInfo;
  private static List<BlockExecutionInfo> blockExecutionInfos;
  private static CarbonTableIdentifier carbonTableIdentifier;
  private static CarbondataColumnHandle carbondataColumnHandle;

  @BeforeClass public static void setUp() {
    ColumnSchema carbonSchema = new ColumnSchema();
    carbonSchema.setColumnName("id");
    carbonSchema.setColumnUniqueId(UUID.randomUUID().toString());
    carbonSchema.setDataType(DataType.INT);
    TableSchema tableSchema = new TableSchema();
    List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
    columnSchemaList.add(carbonSchema);
    tableSchema.setListOfColumns(columnSchemaList);
    tableSchema.setTableName("table1");
    tableInfo = new TableInfo();

    tableInfo.setDatabaseName("schema1");
    tableInfo.setLastUpdatedTime(1234L);
    tableInfo.setTableUniqueName("schema1_tableName");
    tableInfo.setFactTable(tableSchema);
    tableInfo.setStorePath("storePath");
    carbonTable = CarbonTable.buildFromTableInfo(tableInfo);
    schemaTable = new SchemaTableName("schemaName", "tableName");
    domain = TupleDomain.all();
    localSplits = new CarbonLocalInputSplit("segmentId", "path", 0, 5, new ArrayList<String>(), 5,
        Short.MAX_VALUE);
    Optional<Domain> domainAll = Optional.of(Domain.all(IntegerType.INTEGER));

    CarbondataColumnConstraint constraints = new CarbondataColumnConstraint("", domainAll, false);
    List constraintsList = new ArrayList<CarbondataColumnConstraint>();
    constraintsList.add(constraints);
    queryModel = new QueryModel();
    new MockUp<ColumnDetailQueryExecutor>() {
      @Mock public CarbonIterator execute(QueryModel queryModel) {
        blockExecutionInfo = new BlockExecutionInfo();
        blockExecutionInfos = new ArrayList<>();
        ExecutorService executorService = new DefaultEventExecutorGroup(1);
        blockExecutionInfos.add(blockExecutionInfo);
        return new ColumnBasedResultIterator(blockExecutionInfos, queryModel, executorService);
      }
    };
    new MockUp<DataBlockIteratorImpl>() {
      @Mock public List<Object[]> processNextColumnBatch() {
        Object[] object = new Object[] { 4 };
        List<Object[]> resultList = new ArrayList();
        resultList.add(object);
        return resultList;

      }
    };
    new MockUp<CarbonDataBlockIterator>() {
      @Mock protected boolean updateScanner() {
        return true;

      }
    };
    new MockUp<BlockExecutionInfo>() {
      @Mock public QueryDimension[] getQueryDimensions() {
        QueryDimension queryDimension = new QueryDimension("emp");
        QueryDimension[] queryDimensionList = new QueryDimension[] { queryDimension };
        return queryDimensionList;

      }
    };
    new MockUp<ColumnBasedResultCollector>() {
      @Mock private void initDimensionAndMeasureIndexesForFillingData() {

      }
    };
    new MockUp<AbstractDetailQueryResultIterator>() {
      @Mock private void intialiseInfos() {

      }

      @Mock protected void initQueryStatiticsModel() {

      }
    };
    split = new CarbondataSplit("conid", schemaTable, domain, localSplits, constraintsList);
    carbonTableIdentifier = new CarbonTableIdentifier("default", "emp", "1");
    queryModel
        .setAbsoluteTableIdentifier(new AbsoluteTableIdentifier("/default", carbonTableIdentifier));



  }

  @Test(expected=RuntimeException.class) public void testGenNextPage() {
    //case when type is not specified
    carbondataRecordSet =
            new CarbondataRecordSet(carbonTable, null, split, new ArrayList<CarbondataColumnHandle>(),
                    queryModel);
    carbonPage = new CarbondataPageSource(carbondataRecordSet);
    assertNotNull(carbonPage.getNextPage());

    //when type is specified but column batch is null
    new MockUp<ColumnSchema>() {
      @Mock
      public DataType getDataType() {
        return DataType.INT;
      }
    };
    new MockUp<DataBlockIteratorImpl>() {
      @Mock
      public List<Object[]> processNextColumnBatch() {
        Object[] object = new Object[]{4};
        List<Object[]> resultList = new ArrayList();
        resultList.add(object);
        return resultList;

      }
    };
    ColumnSchema columnSchema = new ColumnSchema();
    Type spiType = CarbondataMetadata.carbonDataType2SpiMapper(columnSchema);
    carbondataColumnHandle =
            new CarbondataColumnHandle("connectorId", "id", spiType, 0, 3, 1, true, 1, "int", true, 5,
                    4);
    List<CarbondataColumnHandle> carbonColumnHandles = new ArrayList<>();
    carbonColumnHandles.add(carbondataColumnHandle);
    // carbonColumnHandles.add(carbondataColumnHandle);
    carbondataRecordSet =
            new CarbondataRecordSet(carbonTable, null, split, carbonColumnHandles, queryModel);
    carbonPage = new CarbondataPageSource(carbondataRecordSet);
    assertNotNull(carbonPage.getNextPage());
    new MockUp<BatchResult>() {
     @Mock public List<Object[]> getRows() {
        throw new RuntimeException("Unable to fetch Row");
      }
    };
    carbonPage.getNextPage();
  }
  @AfterClass public static void tearDown(){
    carbonPage.close();
  }
}

