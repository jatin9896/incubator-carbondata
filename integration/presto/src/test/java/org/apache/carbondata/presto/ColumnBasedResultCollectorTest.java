package org.apache.carbondata.presto;


import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.collector.impl.AbstractScannedResultCollector;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.executor.infos.DimensionInfo;
import org.apache.carbondata.core.scan.executor.infos.MeasureInfo;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryMeasure;
import org.apache.carbondata.core.scan.result.AbstractScannedResult;
import org.apache.carbondata.core.scan.result.BatchResult;
import org.apache.carbondata.core.scan.result.impl.NonFilterQueryScannedResult;
import org.apache.carbondata.presto.processor.impl.ColumnBasedResultCollector;
import org.apache.carbondata.presto.scan.result.ColumnBasedResultIterator;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class ColumnBasedResultCollectorTest {
       @BeforeClass
    public static void setUp() {

             }

    @Test
    public void testCollectData() {
        ColumnSchema dimensionColumnSchema=new ColumnSchema();
        dimensionColumnSchema.setDataType(DataType.STRING);
        QueryDimension queryDimension=new QueryDimension("dimmensionColumn");
        CarbonDimension dimension=new CarbonDimension(dimensionColumnSchema,0,0,0,0);
        queryDimension.setDimension(dimension);
        QueryDimension[] queryDimensions=new QueryDimension[2];
        queryDimensions[0]=queryDimension;
        queryDimensions[1]=queryDimension;
        ColumnSchema measureColumnSchema=new ColumnSchema();
        measureColumnSchema.setDataType(DataType.INT);
        QueryMeasure queryMeasure=new QueryMeasure("measureColumn");
        queryMeasure.setMeasure(new CarbonMeasure(measureColumnSchema,1));
        QueryMeasure[] queryMeasures=new QueryMeasure[2];
        queryMeasures[0]=queryMeasure;
        queryMeasures[1]=queryMeasure;
        BlockExecutionInfo blockExecutionInfo = new BlockExecutionInfo();
        blockExecutionInfo.setQueryMeasures(queryMeasures);
        blockExecutionInfo.setQueryDimensions(queryDimensions);
        MeasureInfo measureInfo=new MeasureInfo();
        blockExecutionInfo.setMeasureInfo(measureInfo);
      //  blockExecutionInfo.setDimensionInfo(new DimensionInfo());
        new MockUp<ColumnSchema>(){
            @Mock public List<Encoding> getEncodingList() {
                List encodingList= new ArrayList<>();
                encodingList.add(Encoding.DIRECT_DICTIONARY);
                return encodingList;
            }
        };
        ColumnBasedResultCollector columnBasedResultCollector = new ColumnBasedResultCollector(blockExecutionInfo);
        NonFilterQueryScannedResult nonFilterQueryScannedResult = new NonFilterQueryScannedResult(blockExecutionInfo);
        new MockUp<AbstractScannedResult>(){
          @Mock  public int numberOfOutputRows() {
                return 10;
            }
        };
        new MockUp<NonFilterQueryScannedResult>(){
            @Mock  public boolean hasNext() {
                return true;
            }
        };
        new MockUp<NonFilterQueryScannedResult>(){
            @Mock  public int[] getDictionaryKeyIntegerArray(){
                int dictionaryKeyArray[]=new int[10];
                return dictionaryKeyArray;
            }
        };
        new MockUp<NonFilterQueryScannedResult>(){
            @Mock  byte[][] getComplexTypeKeyArray(){
                byte byteArray[][]=new byte[2][];
                byteArray[0]=new byte[1];
                byteArray[1]=new byte[1];
                return byteArray;
            }
        };
        new MockUp<NonFilterQueryScannedResult>(){
            @Mock  byte[][] getNoDictionaryKeyArray(){
                byte byteArray[][]=new byte[2][];
                byteArray[0]=new byte[1];
                byteArray[1]=new byte[1];
                return byteArray;
            }
        };
        new MockUp<MeasureInfo>(){
            @Mock public DataType[] getMeasureDataTypes(){
              DataType[] dataTypes=new DataType[1];
              dataTypes[0]=DataType.INT;
              return dataTypes;
            }
        };
        new MockUp<AbstractScannedResultCollector>(){
          @Mock  void fillMeasureData(Object[] msrValues, int offset,
                                 AbstractScannedResult scannedResult){

            }
        };

        List data = columnBasedResultCollector.collectData(nonFilterQueryScannedResult, 2);

        assertTrue(data instanceof List);
    }
}
