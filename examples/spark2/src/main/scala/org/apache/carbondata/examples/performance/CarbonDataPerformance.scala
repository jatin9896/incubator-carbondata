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

package org.apache.carbondata.examples.performance

import java.io.File

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object CarbonDataPerformance {

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/integration/presto/test/store"
    val warehouse = s"$rootPath/integration/presto/test/warehouse"
    val metastoredb = s"$rootPath/integration/presto/test"

    val csvRootPath = "/home/pallavi/data_csv/data_csv/test"

    val customerCsvPath = s"$csvRootPath/customer.csv"
    val lineItemCsvPath = s"$csvRootPath/lineitem.csv"
    val nationCsvPath = s"$csvRootPath/nation.csv"
    val ordersCsvPath = s"$csvRootPath/orders.csv"
    val partCsvPath = s"$csvRootPath/part.csv"
    val partSupCsvPath = s"$csvRootPath/partsupp.csv"
    val regionCsvPath = s"$csvRootPath/region.csv"
    val supplierCsvPath = s"$csvRootPath/supplier.csv"

    val filePath = s"$rootPath/integration/presto/data/CarbonDataBenchmarkingResults.txt"

    val queryList: List[String] = QueryUtil.queryList

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")

    import org.apache.spark.sql.CarbonSession._

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("WARN")

    spark.sql("DROP TABLE IF EXISTS NATION").show()
    spark.sql("DROP TABLE IF EXISTS REGION").show()
    spark.sql("DROP TABLE IF EXISTS PART").show()
    spark.sql("DROP TABLE IF EXISTS PARTSUPP").show()
    spark.sql("DROP TABLE IF EXISTS SUPPLIER").show()
    spark.sql("DROP TABLE IF EXISTS CUSTOMER").show()
    spark.sql("DROP TABLE IF EXISTS ORDERS").show()
    spark.sql("DROP TABLE IF EXISTS LINEITEM").show()


    // Create table
    spark.sql("CREATE TABLE NATION  ( " +
              "N_NATIONKEY  INT," +
              "N_NAME STRING," +
              "N_REGIONKEY  INT," +
              "N_COMMENT STRING) " +
              "STORED BY 'carbondata'").show()

    spark.sql("CREATE TABLE REGION ( " +
              "R_REGIONKEY INT ," +
              "\n R_NAME STRING ," +
              "\n R_COMMENT STRING) " +
              "STORED BY 'carbondata'").show()

    spark.sql("CREATE TABLE PART ( " +
              "P_PARTKEY INT ," +
              "\n P_NAME STRING ," +
              "\n P_MFGR STRING ," +
              "\n P_BRAND " +
              "STRING ," +
              "\n P_TYPE STRING ," +
              "\n P_SIZE INT ," +
              "\n P_CONTAINER STRING ," +
              "\n P_RETAILPRICE " +
              "DECIMAL(15,2) ," +
              "\n P_COMMENT STRING ) " +
              "STORED BY 'carbondata'").show()

    spark.sql("CREATE TABLE SUPPLIER ( " +
              "S_SUPPKEY INT ," +
              "\n S_NAME STRING ," +
              "\n S_ADDRESS STRING ,\n " +
              "S_NATIONKEY INT ," +
              "\n S_PHONE STRING ," +
              "\n S_ACCTBAL DECIMAL(15,2) ," +
              "\n S_COMMENT " +
              "STRING )" +
              " STORED BY 'carbondata'").show()

    spark.sql("CREATE TABLE PARTSUPP ( " +
              "PS_PARTKEY INT ," +
              "\n PS_SUPPKEY INT ," +
              "\n PS_AVAILQTY INT ," +
              "\n " +
              "PS_SUPPLYCOST DECIMAL(15,2) ," +
              "\n PS_COMMENT STRING ) " +
              "STORED BY 'carbondata'").show()

    spark.sql("CREATE TABLE CUSTOMER ( " +
              "C_CUSTKEY INT ," +
              "\n C_NAME STRING ," +
              "\n C_ADDRESS STRING ," +
              "\n C_NATIONKEY INT ," +
              "\n C_PHONE STRING ," +
              "\n C_ACCTBAL DECIMAL(15,2) ," +
              "\n C_MKTSEGMENT " +
              "STRING ," +
              "\n C_COMMENT STRING ) " +
              "STORED BY 'carbondata'").show()

    spark.sql("CREATE TABLE ORDERS ( " +
              "O_ORDERKEY INT ," +
              "\n O_CUSTKEY INT ," +
              "\n O_ORDERSTATUS STRING ," +
              "\n O_TOTALPRICE DECIMAL(15,2) ," +
              "\n O_ORDERDATE DATE ," +
              "\n O_ORDERPRIORITY STRING" +
              " , \n O_CLERK STRING , " +
              "\n O_SHIPPRIORITY INT ," +
              "\n O_COMMENT STRING ) " +
              "STORED BY 'carbondata'").show()

    spark.sql("CREATE TABLE LINEITEM ( " +
              "L_ORDERKEY INT ," +
              "\n L_PARTKEY INT ," +
              "\n L_SUPPKEY INT ,\n " +
              "L_LINENUMBER INT ," +
              "\n L_QUANTITY DECIMAL(15,2) ," +
              "\n L_EXTENDEDPRICE DECIMAL(15,2) ," +
              "\n L_DISCOUNT DECIMAL(15,2) ," +
              "\n L_TAX DECIMAL(15,2) ," +
              "\n L_RETURNFLAG STRING ,\n " +
              "L_LINESTATUS STRING ," +
              "\n L_SHIPDATE DATE ," +
              "\n L_COMMITDATE DATE ,\n " +
              "L_RECEIPTDATE DATE ," +
              "\n L_SHIPINSTRUCT STRING ," +
              "\n L_SHIPMODE STRING ,\n " +
              "L_COMMENT STRING ) " +
              "STORED BY 'carbondata'").show()

    spark.sql(s"LOAD DATA LOCAL INPATH '$nationCsvPath' INTO TABLE nation " +
              "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='N_NATIONKEY,N_NAME," +
              "N_REGIONKEY,N_COMMENT')").show()

    spark.sql(s"LOAD DATA LOCAL INPATH '$regionCsvPath' INTO TABLE region " +
              "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='R_REGIONKEY,R_NAME," +
              "R_COMMENT')").show()

    spark.sql(s"LOAD DATA LOCAL INPATH '$partCsvPath' INTO TABLE part OPTIONS" +
              "('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='P_PARTKEY,P_NAME,P_MFGR,P_BRAND," +
              "P_TYPE,P_SIZE,P_CONTAINER,P_RETAILPRICE,P_COMMENT')").show()

    spark.sql(s"LOAD DATA LOCAL INPATH '$supplierCsvPath' INTO TABLE supplier " +
              "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'=' S_SUPPKEY,             " +
              "S_NAME,S_ADDRESS,S_NATIONKEY,S_PHONE,S_ACCTBAL,S_COMMENT')").show()

    spark.sql(s"LOAD DATA LOCAL INPATH '$partSupCsvPath' INTO TABLE partsupp " +
              "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='PS_PARTKEY,PS_SUPPKEY ," +
              "PS_AVAILQTY,PS_SUPPLYCOST,PS_COMMENT')").show()

    spark.sql(s"LOAD DATA LOCAL INPATH '$customerCsvPath' INTO TABLE customer " +
              "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"' , 'FILEHEADER'='C_CUSTKEY,C_NAME," +
              "C_ADDRESS,C_NATIONKEY,C_PHONE,C_ACCTBAL,C_MKTSEGMENT,C_COMMENT')").show()

    spark.sql(s"LOAD DATA LOCAL INPATH '$ordersCsvPath' INTO TABLE orders " +
              "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='O_ORDERKEY,O_CUSTKEY," +
              "O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY," +
              "O_COMMENT')").show()

    spark.sql(s"LOAD DATA LOCAL INPATH '$lineItemCsvPath' INTO TABLE lineitem " +
              "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'=' L_ORDERKEY,L_PARTKEY," +
              "L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG," +
              "L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE," +
              "L_COMMENT')").show()

    spark.sql("SELECT * FROM NATION").show()
    spark.sql("SELECT * FROM REGION").show()
    spark.sql("SELECT * FROM PART").show()
    spark.sql("SELECT * FROM SUPPLIER").show()
    spark.sql("SELECT * FROM PARTSUPP").show()
    spark.sql("SELECT * FROM CUSTOMER").show()
    spark.sql("SELECT * FROM ORDERS").show()
    spark.sql("SELECT * FROM LINEITEM").show()
    spark.sql("drop table if exists q18_large_volume_customer_cached").show()

    Try {
      QueryUtil.evaluateTimeForQuery(queryList, spark)
    } match {
      case Success(queryAndTimeList) => println("\n\n\n Estimated time for queries (in seconds)")
        queryAndTimeList foreach { queryAndTime: (String, Double) =>
          println(
            "\n\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
          println(queryAndTime)

          QueryUtil.writeResults("" + queryAndTime._2 + "\n", filePath)
          println(
            "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        }
      case Failure(exception) =>
        exception.printStackTrace()
        println(" Exception occured " + exception.getMessage)
    }

    spark.stop()
    System.exit(0)
  }

}