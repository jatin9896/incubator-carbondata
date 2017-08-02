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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

// This is used to capture the performance of Parquet on Spark.
object ParquetPerformance {

  def main(args: Array[String]): Unit = {

    val LOGGER = LogServiceFactory.getLogService(ParquetPerformance.getClass.getName)

    val queryList: List[String] = QueryUtil.queryList

    import org.apache.spark.sql.CarbonSession._
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath

    val csvRootPath = "/home/pallavi/data_csv/data_csv/test"

    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metaStoreDb = s"$rootPath/examples/spark2/target/metastore_db"
    val customerCsvPath = s"$csvRootPath/customer.csv"
    val lineItemCsvPath = s"$csvRootPath/lineitem.csv"
    val nationCsvPath = s"$csvRootPath/nation.csv"
    val ordersCsvPath = s"$csvRootPath/orders.csv"
    val partCsvPath = s"$csvRootPath/part.csv"
    val partSupCsvPath = s"$csvRootPath/partsupp.csv"
    val regionCsvPath = s"$csvRootPath/region.csv"
    val supplierCsvPath = s"$csvRootPath/supplier.csv"

    val filePath = s"$rootPath/integration/presto/data/ParquetBenchmarkingResults.txt"

    // clean data folder
    if (true) {
      val clean = (path: String) => FileUtils.deleteDirectory(new File(path))
      clean(storeLocation)
      clean(warehouse)
      clean(metaStoreDb)
    }

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .getOrCreateCarbonSession(storeLocation, metaStoreDb)

    CarbonProperties.getInstance()
      .addProperty("carbon.storelocation", storeLocation)
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")

    spark.sparkContext.setLogLevel("warn")

    LOGGER
      .info(
        "\n\n --------------------------- Creating table REGION ---------------------------------")
    spark.sql(
      """
        |drop table if exists REGIONTEXTFILE
      """.stripMargin)

    spark.sql(
      """
        |drop table if exists REGION
      """.stripMargin
    )

    spark.sql(
      s"""CREATE TABLE REGION ( R_REGIONKEY INT ,
         | R_NAME STRING ,
         | R_COMMENT STRING) stored as orc
    """.stripMargin
    ).show()

    spark.sql(
      s"""CREATE TABLE REGIONTEXTFILE ( R_REGIONKEY INT ,
         | R_NAME STRING ,
         | R_COMMENT STRING) row format delimited fields terminated by '|' stored as textfile
    """.stripMargin
    ).show()

    spark.sql(
      s"""
         | LOAD DATA LOCAL inpath '${ regionCsvPath }' INTO table REGIONTEXTFILE
       """.stripMargin
    ).show()

    spark.sql(
      s"""
         |insert overwrite table REGION select * from REGIONTEXTFILE
       """.stripMargin).show()

    spark.sql(
      """
        |select * from REGION
      """.stripMargin).show()

    LOGGER
      .info(
        "\n\n --------------------------- Creating table NATION ---------------------------------")

    spark.sql(
      """
        |drop table if exists NATIONTEXTFILE
      """.stripMargin)

    spark.sql(
      """
        |drop table if exists NATION
      """.stripMargin
    )

    spark.sql(
      s"""CREATE TABLE NATION ( N_NATIONKEY INT,
         | N_NAME STRING,
         | N_REGIONKEY INT,
         | N_COMMENT STRING) stored as orc
    """.stripMargin
    )

    spark.sql(
      s"""CREATE TABLE NATIONTEXTFLE ( N_NATIONKEY INT,
         | N_NAME STRING,
         | N_REGIONKEY INT,
         | N_COMMENT STRING) row format delimited fields terminated by '|' stored as textfile
    """.stripMargin
    )

    spark.sql(
      s"""
         | LOAD DATA LOCAL inpath '${ nationCsvPath }' INTO table NATIONTEXTFLE
       """.stripMargin
    ).show()

    spark.sql(
      s"""
         |insert overwrite table NATION select * from NATIONTEXTFLE
       """.stripMargin).show()

    spark.sql(
      """
        |select * from NATION
      """.stripMargin).show()


    LOGGER
      .info("\n\n --------------------------- Creating table PART " +
            "---------------------------------")

    spark.sql(
      """
        |drop table if exists PARTTEXTFILE
      """.stripMargin)

    spark.sql(
      """
        |drop table if exists PART
      """.stripMargin
    )

    spark.sql(
      """
        |CREATE TABLE PART ( P_PARTKEY INT ,
        | P_NAME STRING ,
        | P_MFGR STRING ,
        | P_BRAND STRING ,
        | P_TYPE STRING ,
        | P_SIZE INT ,
        | P_CONTAINER STRING ,
        | P_RETAILPRICE DECIMAL(15,2) ,
        | P_COMMENT STRING ) stored as orc
      """.stripMargin
    ).show()

    spark.sql(
      """
        |CREATE TABLE PARTTEXTFILE ( P_PARTKEY INT ,
        | P_NAME STRING ,
        | P_MFGR STRING ,
        | P_BRAND STRING ,
        | P_TYPE STRING ,
        | P_SIZE INT ,
        | P_CONTAINER STRING ,
        | P_RETAILPRICE DECIMAL(15,2) ,
        | P_COMMENT STRING ) row format delimited fields terminated by '|' stored as textfile
      """.stripMargin
    ).show()

    spark.sql(
      s"""
         | LOAD DATA LOCAL inpath '${ partCsvPath }' INTO table PARTTEXTFILE
       """.stripMargin
    ).show()

    spark.sql(
      s"""
         |insert overwrite table PART select * from PARTTEXTFILE
       """.stripMargin).show()

    spark.sql(
      """
        |select * from PART
      """.stripMargin).show()

    LOGGER
      .info(
        "\n\n --------------------------- Creating table SUPPLIER " +
        "---------------------------------")

    spark.sql(
      """
        |drop table if exists SUPPLIERTEXTFILE
      """.stripMargin)

    spark.sql(
      """
        |drop table if exists SUPPLIER
      """.stripMargin
    )

    spark.sql(
      s"""
         | CREATE TABLE SUPPLIER ( S_SUPPKEY INT ,
         | S_NAME STRING ,
         | S_ADDRESS STRING ,
         | S_NATIONKEY INT ,
         | S_PHONE STRING ,
         | S_ACCTBAL DECIMAL(15,2) ,
         | S_COMMENT STRING ) stored as orc
      """.stripMargin
    )

    spark.sql(
      s"""
         | CREATE TABLE SUPPLIERTEXTFILE ( S_SUPPKEY INT ,
         | S_NAME STRING ,
         | S_ADDRESS STRING ,
         | S_NATIONKEY INT ,
         | S_PHONE STRING ,
         | S_ACCTBAL DECIMAL(15,2) ,
         | S_COMMENT STRING ) row format delimited fields terminated by '|' stored as textfile
      """.stripMargin
    )

    spark.sql(
      s"""
         | LOAD DATA LOCAL inpath '${ supplierCsvPath }' INTO table SUPPLIERTEXTFILE
       """.stripMargin
    ).show()

    spark.sql(
      s"""
         |insert overwrite table SUPPLIER select * from SUPPLIERTEXTFILE
       """.stripMargin).show()

    spark.sql(
      """
        |select * from SUPPLIER
      """.stripMargin).show()

    LOGGER
      .info(
        "\n\n\n\n --------------------------- Creating table PARTSUPP " +
        "---------------------------------")

    spark.sql(
      """
        |drop table if exists PARTSUPPTEXTFILE
      """.stripMargin)

    spark.sql(
      """
        |drop table if exists PARTSUPP
      """.stripMargin
    )

    spark.sql(
      s"""
         | CREATE TABLE PARTSUPP ( PS_PARTKEY INT ,
         | PS_SUPPKEY INT ,
         | PS_AVAILQTY INT ,
         | PS_SUPPLYCOST DECIMAL(15,2) ,
         | PS_COMMENT STRING ) stored as orc
      """.stripMargin
    ).show()

    spark.sql(
      s"""
         | CREATE TABLE PARTSUPPTEXTFILE ( PS_PARTKEY INT ,
         | PS_SUPPKEY INT ,
         | PS_AVAILQTY INT ,
         | PS_SUPPLYCOST DECIMAL(15,2) ,
         | PS_COMMENT STRING ) row format delimited fields terminated by '|' stored as textfile
      """.stripMargin
    ).show()

    spark.sql(
      s"""
         | LOAD DATA LOCAL inpath '${ partSupCsvPath }' INTO table PARTSUPPTEXTFILE
       """.stripMargin
    ).show()

    spark.sql(
      s"""
         |insert overwrite table PARTSUPP select * from PARTSUPPTEXTFILE
       """.stripMargin).show()

    spark.sql(
      """
        |select * from PARTSUPP
      """.stripMargin).show()

    LOGGER
      .info(
        "\n\n --------------------------- Creating table CUSTOMER " +
        "---------------------------------")

    spark.sql(
      """
        |drop table if exists CUSTOMERTEXTFILE
      """.stripMargin)

    spark.sql(
      """
        |drop table if exists CUSTOMER
      """.stripMargin
    )

    spark.sql(
      s"""
         | CREATE TABLE CUSTOMER ( C_CUSTKEY INT ,
         | C_NAME STRING ,
         | C_ADDRESS STRING ,
         | C_NATIONKEY INT ,
         | C_PHONE STRING ,
         | C_ACCTBAL DECIMAL(15,2) ,
         | C_MKTSEGMENT STRING ,
         | C_COMMENT STRING ) stored as orc
      """.stripMargin
    ).show()

    spark.sql(
      s"""
         | CREATE TABLE CUSTOMERTEXTFILE ( C_CUSTKEY INT ,
         | C_NAME STRING ,
         | C_ADDRESS STRING ,
         | C_NATIONKEY INT ,
         | C_PHONE STRING ,
         | C_ACCTBAL DECIMAL(15,2) ,
         | C_MKTSEGMENT STRING ,
         | C_COMMENT STRING ) row format delimited fields terminated by '|' stored as textfile
      """.stripMargin
    ).show()

    spark.sql(
      s"""
         | LOAD DATA LOCAL inpath '${ customerCsvPath }' INTO table CUSTOMERTEXTFILE
       """.stripMargin
    ).show()

    spark.sql(
      s"""
         |insert overwrite table CUSTOMER select * from CUSTOMERTEXTFILE
       """.stripMargin).show()

    spark.sql(
      """
        |select * from CUSTOMER
      """.stripMargin).show()

    LOGGER
      .info(
        "\n\n --------------------------- Creating table ORDERS ---------------------------------")

    spark.sql(
      """
        |drop table if exists ORDERSTEXTFILE
      """.stripMargin)

    spark.sql(
      """
        |drop table if exists ORDERS
      """.stripMargin
    )

    spark.sql(
      s"""
         | CREATE TABLE ORDERS ( O_ORDERKEY INT ,
         | O_CUSTKEY INT ,
         | O_ORDERSTATUS STRING ,
         | O_TOTALPRICE DECIMAL(15,2) ,
         | O_ORDERDATE Date ,
         | O_ORDERPRIORITY STRING ,
         | O_CLERK STRING ,
         | O_SHIPPRIORITY INT ,
         | O_COMMENT STRING ) stored as orc
      """.stripMargin
    ).show()

    spark.sql(
      s"""
         | CREATE TABLE ORDERSTEXTFILE ( O_ORDERKEY INT ,
         | O_CUSTKEY INT ,
         | O_ORDERSTATUS STRING ,
         | O_TOTALPRICE DECIMAL(15,2) ,
         | O_ORDERDATE Date ,
         | O_ORDERPRIORITY STRING ,
         | O_CLERK STRING ,
         | O_SHIPPRIORITY INT ,
         | O_COMMENT STRING ) row format delimited fields terminated by '|' stored as textfile
      """.stripMargin
    ).show()

    spark.sql(
      s"""
         | LOAD DATA LOCAL inpath '${ ordersCsvPath }' INTO table ORDERSTEXTFILE
       """.stripMargin
    ).show()

    spark.sql(
      s"""
         |insert overwrite table ORDERS select * from ORDERSTEXTFILE
       """.stripMargin).show()

    spark.sql(
      """
        |select * from ORDERS
      """.stripMargin).show()

    LOGGER
      .info(
        "\n\n --------------------------- Creating table LINEITEM " +
        "---------------------------------")

    spark.sql(
      """
        |drop table if exists LINEITEMTEXTFILE
      """.stripMargin)

    spark.sql(
      """
        |drop table if exists LINEITEM
      """.stripMargin
    )

    spark.sql(
      s"""
         | CREATE TABLE LINEITEM ( L_ORDERKEY INT ,
         | L_PARTKEY INT ,
         | L_SUPPKEY INT ,
         | L_LINENUMBER INT ,
         | L_QUANTITY DECIMAL(15,2) ,
         | L_EXTENDEDPRICE DECIMAL(15,2) ,
         | L_DISCOUNT DECIMAL(15,2) ,
         | L_TAX DECIMAL(15,2) ,
         | L_RETURNFLAG STRING ,
         | L_LINESTATUS STRING ,
         | L_SHIPDATE Date ,
         | L_COMMITDATE Date ,
         | L_RECEIPTDATE Date ,
         | L_SHIPINSTRUCT STRING ,
         | L_SHIPMODE STRING ,
         | L_COMMENT STRING ) stored as orc
      """.stripMargin
    ).show()

    spark.sql(
      s"""
         | CREATE TABLE LINEITEMTEXTFILE ( L_ORDERKEY INT ,
         | L_PARTKEY INT ,
         | L_SUPPKEY INT ,
         | L_LINENUMBER INT ,
         | L_QUANTITY DECIMAL(15,2) ,
         | L_EXTENDEDPRICE DECIMAL(15,2) ,
         | L_DISCOUNT DECIMAL(15,2) ,
         | L_TAX DECIMAL(15,2) ,
         | L_RETURNFLAG STRING ,
         | L_LINESTATUS STRING ,
         | L_SHIPDATE Date ,
         | L_COMMITDATE Date ,
         | L_RECEIPTDATE Date ,
         | L_SHIPINSTRUCT STRING ,
         | L_SHIPMODE STRING ,
         | L_COMMENT STRING ) row format delimited fields terminated by '|' stored as textfile
      """.stripMargin
    ).show()

    spark.sql(
      s"""
         | LOAD DATA LOCAL inpath '${ lineItemCsvPath }' INTO table LINEITEMTEXTFILE
       """.stripMargin
    ).show()

    spark.sql(
      s"""
         |insert overwrite table LINEITEM select * from LINEITEMTEXTFILE
       """.stripMargin).show()

    spark.sql(
      """
        |select * from LINEITEM
      """.stripMargin).show()

    Try {
      QueryUtil.evaluateTimeForQuery(queryList, spark)
    } match {
      case Success(queryAndTimeList) => println("\n\n\n Estimated time for queries (in seconds)")
        queryAndTimeList foreach { queryAndTime: (String, Double) =>
          println(
            "\n\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
          println(queryAndTime)

        //  QueryUtil.writeResults("" + queryAndTime._2 + "\n", filePath)
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