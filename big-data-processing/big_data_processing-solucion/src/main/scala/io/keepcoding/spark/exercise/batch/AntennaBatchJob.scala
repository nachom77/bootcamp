package io.keepcoding.spark.exercise.batch

import io.keepcoding.spark.exercise.streaming.AntennaStreamingJob.spark
import org.apache.avro.generic.GenericData.StringType

import java.time.OffsetDateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AntennaBatchJob extends BatchJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(s"${storagePath}")
      .filter(
        $"year" === lit(filterDate.getYear) &&
          $"month" === lit(filterDate.getMonthValue) &&
          $"day" === lit(filterDate.getDayOfMonth) &&
          $"hour" === lit(filterDate.getHour)
      )
  }

  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF.as("antenna")
      .join(
        metadataDF.as("metadata"),
        $"antenna.id" === $"metadata.id"
      ).drop($"metadata.id")
  }



  override def computeDevicesByAntenna_Id(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"bytes", $"antenna_id")
      .groupBy($"antenna_id", window($"timestamp", "1 hour"))
      .agg(sum("bytes").as("value"))
      .select( $"window.start".as("timestamp"), $"value", $"antenna_id".as("ID"))
      .withColumn("type", lit("TotalByAntenna_id"))
  }

  override def computeDevicesCountByMail(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"bytes", $"email")
      .groupBy($"email", window($"timestamp", "1 hour"))
      .agg(sum("bytes").as("value"))
      .select( $"window.start".as("timestamp"), $"value", $"email".as("ID"))
      .withColumn("type", lit("TotalByEmail"))
  }

  override def computeDevicesUse(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp",$"bytes",$"email",$"quota")
      .groupBy($"email", window($"timestamp", "1 hour"))
      .agg(sum("bytes").as("usage"), avg($"quota").as("quota"))
      .select( $"window.start".as("timestamp"), $"email", $"usage", $"quota" )
      .filter($"usage" > $"quota")
  }


  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .write
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(s"${storageRootPath} /Users/nacho/Desktop/Escritorio - MacBook Pro de MacConta/keepcoding/7 spark scala/tmp/data-spark_1h")
  }

  def main(args: Array[String]): Unit = {

    val rawDF = readFromStorage("/Users/nacho/Desktop/Escritorio - MacBook Pro de MacConta/keepcoding/7 spark scala/tmp/data-spark", OffsetDateTime.parse("2022-02-27T01:00:00Z"))
    val metadataDF = readAntennaMetadata(s"jdbc:postgresql://23.251.148.222:5432/postgres",
      "user_metadata",
      "postgres",
      "keepcoding"
    )

    val enrichDF = enrichAntennaWithMetadata(rawDF, metadataDF)

    writeToJdbc(computeDevicesByAntenna_Id(enrichDF),
      s"jdbc:postgresql://23.251.148.222:5432/postgres",
      "bytes_hourly",
      "postgres",
      "keepcoding"
    )
    writeToJdbc(computeDevicesCountByMail(enrichDF),
      s"jdbc:postgresql://23.251.148.222:5432/postgres",
      "bytes_hourly",
      "postgres",
      "keepcoding"
    )

    writeToJdbc(computeDevicesUse(enrichDF),
      s"jdbc:postgresql://23.251.148.222:5432/postgres",
      "user_quota_limit",
      "postgres",
      "keepcoding"
    )

    //run(args)
  }
}
