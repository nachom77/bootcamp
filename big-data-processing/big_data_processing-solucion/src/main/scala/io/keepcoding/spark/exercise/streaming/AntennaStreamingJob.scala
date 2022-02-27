package io.keepcoding.spark.exercise.streaming

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.concurrent.duration.Duration


object AntennaStreamingJob extends StreamingJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    //.appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val struct = StructType(Seq(
      StructField("timestamp", LongType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("antenna_id", StringType, nullable = false),
      StructField("bytes", LongType, nullable = false),
      StructField("app", StringType, nullable = false),
    ))
    dataFrame
      .select(from_json(col("value").cast(StringType), struct).as("json"))
      .select("json.*")
      .withColumn("timestamp", $"timestamp".cast(TimestampType))
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


  override def computeDevicesCountById(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"bytes", $"id")
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"id", window($"timestamp", "90 seconds"))
      .agg(sum("bytes").as("value"))
      .select( $"window.start".as("timestamp"), $"value",$"id".as("ID").cast(StringType))
      .withColumn("type", lit("TotalById"))
  }


  override def computeDevicesCountByAntenna_Id(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"bytes", $"antenna_id")
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"antenna_id", window($"timestamp", "90 seconds"))
      .agg(sum("bytes").as("value"))
      .select( $"window.start".as("timestamp"), $"value", $"antenna_id".as("ID").cast(StringType))
      .withColumn("type", lit("TotalByAntenna_id"))
  }

  override def computeDevicesCountByApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp",$"bytes",$"app")
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"app", window($"timestamp", "90 seconds"))
      .agg(sum("bytes").as("value"))
      .select( $"window.start".as("timestamp"), $"value",$"app".as("ID").cast(StringType))
      .withColumn("type", lit("TotalByApp"))

  }



  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
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
      .start()
      .awaitTermination()
  }


  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    dataFrame
      .withColumn("year", year($"timestamp"))
      .withColumn("month", month($"timestamp"))
      .withColumn("day", dayofmonth($"timestamp"))
      .withColumn("hour", hour($"timestamp"))
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", storageRootPath)
      .option("checkpointLocation", "/tmp/spark-checkpoint")
      .start()
      .awaitTermination()
  }



  def main(args: Array[String]): Unit = {
    val metadataDF = readAntennaMetadata(
      s"jdbc:postgresql://23.251.148.222:5432/postgres",
      "user_metadata",
      "postgres",
      "keepcoding"
    )


    val future1 = writeToJdbc(computeDevicesCountById(
      enrichAntennaWithMetadata(
        parserJsonData(
          readFromKafka("34.68.239.159:9092", "devices")
        ),
        metadataDF
      )
    ), s"jdbc:postgresql://23.251.148.222:5432/postgres", "bytes", "postgres", "keepcoding")

    val future2 = writeToJdbc(computeDevicesCountByAntenna_Id(
      enrichAntennaWithMetadata(
        parserJsonData(
          readFromKafka("34.68.239.159:9092", "devices")
        ),
        metadataDF
      )
    ), s"jdbc:postgresql://23.251.148.222:5432/postgres", "bytes", "postgres", "keepcoding")

    val future3 = writeToJdbc(computeDevicesCountByApp(
      enrichAntennaWithMetadata(
        parserJsonData(
          readFromKafka("34.68.239.159:9092", "devices")
        ),
        metadataDF
      )
    ), s"jdbc:postgresql://23.251.148.222:5432/postgres", "bytes", "postgres", "keepcoding")

    val future4 = writeToStorage(parserJsonData(readFromKafka("34.68.239.159:9092", "devices")), "/Users/nacho/Desktop/Escritorio - MacBook Pro de MacConta/keepcoding/7 spark scala/tmp/data-spark")

    Await.result(Future.sequence(Seq(future1, future2, future3, future4)), Duration.Inf)



    //run(args)
  }
}
