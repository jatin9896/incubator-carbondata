package org.apache.carbondata.presto;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableMap;
import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.presto.impl.CarbonTableConfig;
import org.apache.carbondata.presto.impl.CarbonTableReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.function.Predicate;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CarbondataMetadataTest {

    CarbondataConnectorId carbondataConnectorId = new CarbondataConnectorId("connectorid");
    CarbonTableReader carbonTableReader = new CarbonTableReader(new CarbonTableConfig());
    CarbondataMetadata carbondataMetadata = new CarbondataMetadata(carbondataConnectorId, carbonTableReader);
    ConnectorSession connectorSession = new ConnectorSession() {
        @Override
        public String getQueryId() {
            return null;
        }

        @Override
        public Identity getIdentity() {
            return null;
        }

        @Override
        public TimeZoneKey getTimeZoneKey() {
            return null;
        }

        @Override
        public Locale getLocale() {
            return null;
        }

        @Override
        public long getStartTime() {
            return 0;
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }
    };


    @Before
    public void setUp() {
        new MockUp<CarbonTableReader>() {
            @Mock
            public List<String> getSchemaNames() {
                List<String> schemaNames = new ArrayList<String>();
                schemaNames.add("schema1");
                return schemaNames;
            }

            @Mock
            public Set<String> getTableNames(String schema) {
                Set<String> tableNames = new HashSet<>();
                tableNames.add("table1");
                return tableNames;
            }
        };

        new MockUp<CarbonTableReader>() {
            @Mock
            public CarbonTable getTable(SchemaTableName schemaTableName) {
                return CarbonTable.buildFromTableInfo(getTableInfo(1000L));
            }
        };

        new MockUp<CarbonTable>() {
            @Mock
            public List<CarbonColumn> getCreateOrderColumn(String tableName) {
                List<CarbonColumn> carbonColumns = new ArrayList<CarbonColumn>();
                ColumnSchema columnSchema = new ColumnSchema();
                columnSchema.setDataType(DataType.INT);
                columnSchema.setColumnName("id");
                carbonColumns.add(new CarbonColumn(columnSchema, 0, 0));
                return carbonColumns;
            }
        };

    }

    @Test
    public void listSchemaNamesTest() {
        List<String> schemaNames = carbondataMetadata.listSchemaNames(connectorSession);
        assertEquals(schemaNames.get(0), "schema1");
    }

    @Test
    public void listTablesTestWhenSchemaNameNotNull() {
        List<SchemaTableName> tableNameList = carbondataMetadata.listTables(connectorSession, "schemaName");
        assertEquals(tableNameList.get(0).getTableName(), "table1");
        assertEquals(tableNameList.get(0).getSchemaName(), "schemaname");
    }

    @Test
    public void listTablesTestWhenSchemaNameIsNull() {
        List<SchemaTableName> tableNameList = carbondataMetadata.listTables(connectorSession, null);
        assertEquals(tableNameList.get(0).getTableName(), "table1");
        assertEquals(tableNameList.get(0).getSchemaName(), "schema1");
    }

    private ColumnSchema getColumnarDimensionColumn() {
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


    private ColumnSchema getColumnarMeasureColumn() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setColumnName("id");
        dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
        dimColumn.setDataType(DataType.INT);
        return dimColumn;
    }

    private TableSchema getTableSchema() {
        TableSchema tableSchema = new TableSchema();
        List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
        columnSchemaList.add(getColumnarMeasureColumn());
        columnSchemaList.add(getColumnarDimensionColumn());
        tableSchema.setListOfColumns(columnSchemaList);
        tableSchema.setTableName("table1");
        return tableSchema;
    }

    private TableInfo getTableInfo(long timeStamp) {
        TableInfo info = new TableInfo();
        info.setDatabaseName("schema1");
        info.setLastUpdatedTime(timeStamp);
        info.setTableUniqueName("schema1_tableName");
        info.setFactTable(getTableSchema());
        info.setStorePath("storePath");
        return info;
    }

    @Test
    public void listTableColumnsTest() {

        Map<SchemaTableName, List<ColumnMetadata>> tableCols = carbondataMetadata.listTableColumns(connectorSession, new SchemaTablePrefix("schema1", "tableName"));
        assertEquals(tableCols.get(new SchemaTableName("schema1", "tableName")).get(0).getName(), "id");
        assertEquals(tableCols.get(new SchemaTableName("schema1", "tableName")).get(0).getType(), IntegerType.INTEGER);
    }

    @Test(expected = SchemaNotFoundException.class)
    public void listTableColumnsTestExceptionCase() {

        new MockUp<CarbonTableReader>() {
            @Mock
            public CarbonTable getTable(SchemaTableName schemaTableName) {
                return CarbonTable.buildFromTableInfo(getTableInfo(1000L));
            }
        };

        new MockUp<SchemaTableName>() {
            @Mock
            public String getSchemaName() {
                return "null";
            }
        };

        carbondataMetadata.listTableColumns(connectorSession, new SchemaTablePrefix("schema1", "tableName"));
    }

    @Test
    public void getColumnHandlesTest() {

        new MockUp<CarbonTableReader>() {
            @Mock
            public CarbonTable getTable(SchemaTableName schemaTableName) {
                return CarbonTable.buildFromTableInfo(getTableInfo(1000L));
            }
        };

        Map<String, ColumnHandle> columnHandleMap = carbondataMetadata.getColumnHandles(connectorSession, new CarbondataTableHandle("connectorId", new SchemaTableName("schema1", "table1")));

        Assert.assertEquals(((CarbondataColumnHandle) columnHandleMap.get("imei")).getColumnType(), VarcharType.VARCHAR);
        assertEquals(((CarbondataColumnHandle) columnHandleMap.get("id")).getColumnType(), IntegerType.INTEGER);
        assertEquals(((CarbondataColumnHandle) columnHandleMap.get("imei")).getConnectorId(), "connectorid");
    }

    @Test(expected = SchemaNotFoundException.class)
    public void getColumnHandlesTestExceptionCase() {

        new MockUp<CarbonTableReader>() {
            @Mock
            public CarbonTable getTable(SchemaTableName schemaTableName) {
                return CarbonTable.buildFromTableInfo(getTableInfo(1000L));
            }
        };

        new MockUp<SchemaTableName>() {
            @Mock
            public String getSchemaName() {
                return "null";
            }
        };

        carbondataMetadata.getColumnHandles(connectorSession, new CarbondataTableHandle("connectorId", new SchemaTableName("schema1", "table1")));
    }

    @Test
    public void getColumnMetadataTest() {
        CarbondataColumnHandle columnHandle = new CarbondataColumnHandle("connectorid", "id", IntegerType.INTEGER, 0, 0, 0, true, 0, "1234567890", true, 0, 0);
        ColumnMetadata columnMetadata = carbondataMetadata.getColumnMetadata(connectorSession, new CarbondataTableHandle("connectorId", new SchemaTableName("schema1", "table1")), columnHandle);
        assertEquals(columnMetadata.getName(), "id");
        assertEquals(columnMetadata.getType(), IntegerType.INTEGER);
    }

    @Test
    public void getTableHandleTest() {
        ConnectorTableHandle carbondataTableHandle = carbondataMetadata.getTableHandle(connectorSession, new SchemaTableName("schema1", "table1"));
        assertTrue(carbondataTableHandle instanceof CarbondataTableHandle);

    }

    @Test
    public void getTableLayoutsTest() {
        ColumnHandle columnHandle = new CarbondataColumnHandle("connectorid", "id", IntegerType.INTEGER, 0, 0, 0, true, 0, "1234567890", true, 0, 0);
        Set<ColumnHandle> columnHandles = new HashSet<>();
        columnHandles.add(columnHandle);
        ImmutableMap<ColumnHandle, NullableValue> bindings = ImmutableMap.<ColumnHandle, NullableValue>builder()
                .put(columnHandle, NullableValue.of(INTEGER, 10L)).build();
        TupleDomain<ColumnHandle> columnHandleTupleDomain = TupleDomain.fromFixedValues(bindings);

        List<ConnectorTableLayoutResult> tableLayoutResults = carbondataMetadata.getTableLayouts(connectorSession, new CarbondataTableHandle("connectorId", new SchemaTableName("schema1", "table1")), new Constraint<ColumnHandle>(columnHandleTupleDomain, convertToPredicate(columnHandleTupleDomain)), Optional.of(columnHandles));

        assertTrue(tableLayoutResults.get(0).getTableLayout().getHandle() instanceof CarbondataTableLayoutHandle);
        assertEquals(tableLayoutResults.get(0).getUnenforcedConstraint(), columnHandleTupleDomain);
    }

    private Predicate<Map<ColumnHandle, NullableValue>> convertToPredicate(TupleDomain<ColumnHandle> tupleDomain) {
        return bindings -> tupleDomain.contains(TupleDomain.fromFixedValues(bindings));
    }

    @Test
    public void getTableLayoutTest() {
        CarbondataTableHandle carbondataTableHandle = new CarbondataTableHandle("connectorId", new SchemaTableName("schema1", "table1"));
        ColumnHandle columnHandle = new CarbondataColumnHandle("connectorid", "id", IntegerType.INTEGER, 0, 0, 0, true, 0, "1234567890", true, 0, 0);
        ImmutableMap<ColumnHandle, NullableValue> bindings = ImmutableMap.<ColumnHandle, NullableValue>builder()
                .put(columnHandle, NullableValue.of(INTEGER, 10L)).build();
        TupleDomain<ColumnHandle> columnHandleTupleDomain = TupleDomain.fromFixedValues(bindings);

        CarbondataTableLayoutHandle carbondataTableLayoutHandle = new CarbondataTableLayoutHandle(carbondataTableHandle, columnHandleTupleDomain);
        ConnectorTableLayout connectorTableLayout = carbondataMetadata.getTableLayout(connectorSession, carbondataTableLayoutHandle);
        assertTrue(connectorTableLayout.getHandle() instanceof CarbondataTableLayoutHandle);
        assertEquals(((CarbondataTableLayoutHandle) connectorTableLayout.getHandle()).getConstraint(), columnHandleTupleDomain);
    }

    @Test
    public void getTableMetadataTest() {

        new MockUp<CarbonTableReader>() {
            @Mock
            public CarbonTable getTable(SchemaTableName schemaTableName) {
                return CarbonTable.buildFromTableInfo(getTableInfo(1000L));
            }
        };

        new MockUp<CarbonTable>() {
            @Mock
            public List<CarbonColumn> getCreateOrderColumn(String tableName) {
                List<CarbonColumn> carbonColumns = new ArrayList<CarbonColumn>();
                ColumnSchema columnSchema = new ColumnSchema();
                columnSchema.setDataType(DataType.LONG);
                columnSchema.setColumnName("id");
                carbonColumns.add(new CarbonColumn(columnSchema, 0, 0));
                return carbonColumns;
            }
        };

        ConnectorTableMetadata connectorTableMetadata = carbondataMetadata.getTableMetadata(connectorSession, new CarbondataTableHandle("connectorId", new SchemaTableName("schema1", "table1")));

        assertEquals(connectorTableMetadata.getColumns().get(0).getName(), "id");
        assertEquals(connectorTableMetadata.getColumns().get(0).getType(), BigintType.BIGINT);
    }
}
