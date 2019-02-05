package com.mpojeda84.mapr.scala


import com.mapr.db.spark.streaming.MapRDBSourceConfig
import com.mpojeda84.mapr.scala.config.Configuration
import org.apache.spark.sql.SparkSession
import com.mpojeda84.mapr.scala.helper.Helper


object Application {

  def main (args: Array[String]): Unit = {

    val argsConfiguration = Configuration.parse(args)

    val spark = SparkSession.builder.appName("OBD Stream to Transformed").getOrCreate

    import spark.implicits._

    val stream = spark.readStream
      .format("kafka")
      .option("failOnDataLoss", false)
      .option("kafka.bootstrap.servers", "none")
      .option("subscribe", argsConfiguration.topic)
      .option("startingOffsets", "earliest")
      .load()


    val documents = stream.select("value").as[String].map(Helper.toJsonWithId)
    documents.createOrReplaceTempView("raw_data")

    saveTransformed(spark,argsConfiguration)

  }

  private def saveTransformed(spark: SparkSession, argsConfiguration: Configuration): Unit = {
    val processed = spark.sql("SELECT VIN AS `vin`, first(make) AS `make`, first(`year`) AS `year`, avg(cast(`speed` AS Double)) AS `avgSpeed`, max(cast(`instantFuelEconomy` AS Double)) AS `bestFuelEconomy`, avg(cast(`instantFuelEconomy` AS Double)) AS `totalFuelEconomy` FROM raw_data GROUP BY vin")

    val query = processed.writeStream
      .format(MapRDBSourceConfig.Format)
      .option(MapRDBSourceConfig.TablePathOption, argsConfiguration.transformed)
      .option(MapRDBSourceConfig.CreateTableOption,false)
      .option(MapRDBSourceConfig.IdFieldPathOption, "vin")
      .option("checkpointLocation", argsConfiguration.checkpoint)
      .outputMode("complete")
      .start()

    query.awaitTermination()
  }


}
