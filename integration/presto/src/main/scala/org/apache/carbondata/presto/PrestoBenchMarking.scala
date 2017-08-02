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

package org.apache.carbondata.presto

import java.io.File
import java.sql.{Connection, DriverManager, ResultSet, SQLException, Statement}
import java.util.{Locale, Optional}

import com.facebook.presto.Session
import com.facebook.presto.execution.QueryIdGenerator
import com.facebook.presto.metadata.SessionPropertyManager
import com.facebook.presto.spi.`type`.TimeZoneKey.UTC_KEY
import com.facebook.presto.spi.security.Identity
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

object PrestoBenchMarking {
  val rootPath: String = new File(this.getClass.getResource("/").getPath
    + "../../../..").getCanonicalPath

  val CARBONDATA_CATALOG = "carbondata"
  val CARBONDATA_CONNECTOR = "carbondata"
  val CARBONDATA_STOREPATH = s"$rootPath/integration/presto/test/store"
  val CARBONDATA_SOURCE = "carbondata"

  // Instantiates the Presto Server to connect with the Apache CarbonData
  /* @throws[Exception]
   def createQueryRunner(extraProperties: util.Map[String, String]): DistributedQueryRunner = {
     val queryRunner = new DistributedQueryRunner(createSession, 4, extraProperties)
     try {
       queryRunner.installPlugin(new CarbondataPlugin)
       val carbonProperties = ImmutableMap.builder[String, String]
         // .put("com.facebook.presto", "DEBUG")
         .put("carbondata-store", CARBONDATA_STOREPATH).build

       // CreateCatalog will create a catalog for CarbonData in etc/catalog.
       queryRunner.createCatalog(CARBONDATA_CATALOG, CARBONDATA_CONNECTOR, carbonProperties)
       queryRunner
     } catch {
       case e: Exception =>
         queryRunner.close()
         throw e
     }
   }
 */
  // CreateSession will create a new session in the Server to connect and execute queries.
  def createSession: Session = {
    Session.builder(new SessionPropertyManager)
      .setQueryId(new QueryIdGenerator().createNextQueryId)
      .setIdentity(new Identity("user", Optional.empty()))
      .setSource(CARBONDATA_SOURCE).setCatalog(CARBONDATA_CATALOG)
      .setTimeZoneKey(UTC_KEY).setLocale(Locale.ENGLISH)
      .setRemoteUserAddress("address")
      .setUserAgent("agent").build
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val rootFilePath = s"$rootPath/integration/presto/data/"
    val prestoFile = s"$rootFilePath/PrestoBenchmarkingResults.txt"
    val util: BenchMarkingUtil.type = BenchMarkingUtil
    // Garbage Collection
    System.gc()

    prestoJdbcClient()

    /* result onSuccess{
       case res=> res.foreach(qtime => println("Time :" + qtime))
     }
     result onFailure{
       case res=>println("Future failed for some queries "+ result)
     }

    *//*result.onComplete {
       case Success(res) => res.foreach(qtime => println("Time :" + qtime))
       case Failure(ex) => Future.successful(0.0)
     }
    Await.ready(result, 3600.seconds)*/

    /*prestoJdbcClient().foreach { (x: Array[Double]) =>
      val y: List[Double] = (x map { (z: Double) => z }).toList
      (BenchMarkingUtil.queries, y).zipped foreach { (query, time) =>

        println(">>>>>>QUERY EXECUTION TIME " + time)
       // util.writeResults(s"\n$time",prestoFile)
          /* util.writeResults(" [ Query :" + query + "\n"
                             + "Time :" + time + " ] \n\n "
             , prestoFile)*/
      }
    }*/
    // scalastyle:off
    // util.readFromFile(prestoFile).foreach(line => println(line))
    // scalastyle:on
    System.exit(0)
  }

  // Creates a JDBC Client to connect CarbonData to Presto
  @throws[Exception]
  def prestoJdbcClient() = {
    val logger: Logger = LoggerFactory.getLogger("Presto Server on CarbonData")

    /*   import scala.collection.JavaConverters._

       val prestoProperties: util.Map[String, String] = Map(("http-server.http.port", "8086")).asJava

       logger.info("======== STARTING PRESTO SERVER ========")
       val queryRunner: DistributedQueryRunner = createQueryRunner(
         prestoProperties)
       /*    val queryRunner: DistributedQueryRunner = createQueryRunner(
             ImmutableMap.of("http-server.http.port", "8086"))*/
       Thread.sleep(10)
       logger.info("STARTED SERVER AT :" + queryRunner.getCoordinator.getBaseUrl)
   */
    // Step 1: Create Connection Strings*/
    val JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver"
    val DB_URL = "jdbc:presto://localhost:8086/carbondata/default"

    // The database Credentials
    val USER = "username"
    val PASS = "password"
    try {
      logger.info("=============Connecting to database/table" +
        "===============")
      // STEP 2: Register JDBC driver
      Class.forName(JDBC_DRIVER)
      // STEP 3: Open a connection
      val conn: Connection = DriverManager.getConnection(DB_URL, USER, PASS)
      val stmt: Statement = conn.createStatement

      val executionTime: Array[Future[Option[Double]]] = BenchMarkingUtil.queries.map { queries =>
        Future {
          val time: Option[Double] = executeQuery(stmt, queries)
          time
        }
      }
      // conn.close()
      val output: Future[List[Option[Double]]] = Future.sequence(executionTime.toList)
      Await.ready(output, 3600.seconds)
      output map {
        out =>
          out.map {
            case Some(time) => logger.info("Future Success with result : " + time)
            case None => logger.info("A Query Failed")
          }
      }

    } catch {
      case se: SQLException =>
        // Handle errors for JDBC
        logger.error(se.getMessage)
      //          None
      case e: Exception =>
        // Handle errors for Class.forName
        logger.error(e.getMessage)
      //          None
    }
  }

  private def executeQuery(statement: Statement, query: Query): Option[Double] = {
    try {
      val time: Double = BenchMarkingUtil.time {

        val res: ResultSet = statement.executeQuery(query.sqlText)

        var i = 0
        while (res.next()) {
          /*  println(res.getString("s_name") + "------" + i)*/
          i = i + 1
        }
      }
      Some(time)
    }
    catch {
      case e: Exception =>
        None
    }
  }
}