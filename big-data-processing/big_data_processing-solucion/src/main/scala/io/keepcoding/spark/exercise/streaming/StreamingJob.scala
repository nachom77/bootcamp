package io.keepcoding.spark.exercise.streaming

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(timestamp: Timestamp, id: String,antenna_id: String, bytes: Long, app: String)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def computeDevicesCountById(dataFrame: DataFrame): DataFrame

  def computeDevicesCountByAntenna_Id(dataFrame: DataFrame): DataFrame

  def computeDevicesCountByApp(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable1,aggJdbcTable2,aggJdbcTable3, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)
    val antennaDF = parserJsonData(kafkaDF)
    val metadataDF = readAntennaMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val antennaMetadataDF = enrichAntennaWithMetadata(antennaDF, metadataDF)
    val storageFuture = writeToStorage(antennaDF, storagePath)
    val aggByIdDF = computeDevicesCountById(antennaMetadataDF)
    val aggByAntennaIdDF = computeDevicesCountByAntenna_Id(antennaMetadataDF)
    val aggByAppDF = computeDevicesCountByApp(antennaMetadataDF)
    val aggFuture1 = writeToJdbc(aggByIdDF, jdbcUri, aggJdbcTable1, jdbcUser, jdbcPassword)
    val aggFuture2 = writeToJdbc(aggByAntennaIdDF, jdbcUri, aggJdbcTable2, jdbcUser, jdbcPassword)
    val aggFuture3 = writeToJdbc(aggByAppDF, jdbcUri, aggJdbcTable3, jdbcUser, jdbcPassword)
    val aggFuture4 = writeToStorage(antennaDF, storagePath)
    Await.result(Future.sequence(Seq(aggFuture1,aggFuture2,aggFuture3, aggFuture4)), Duration.Inf)

    spark.close()
  }

}
