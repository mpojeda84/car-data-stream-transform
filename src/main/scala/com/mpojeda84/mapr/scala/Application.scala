package com.mpojeda84.mapr.scala

import com.mapr.db.spark.streaming.MapRDBSourceConfig
import com.mpojeda84.mapr.scala.config.Configuration
import org.apache.spark.sql.SparkSession
import com.mpojeda84.mapr.scala.helper.Helper

object Application {

  def main (args: Array[String]): Unit = {

    val argsConfiguration = Configuration.parse(args)
    val spark = SparkSession.builder.appName("Car Data stream to table transformed").getOrCreate
    import spark.implicits._

    val stream = spark.readStream.format("kafka").option("failOnDataLoss", false).option("kafka.bootstrap.servers", "none").option("subscribe", argsConfiguration.topic).option("startingOffsets", "earliest").load()
    val documents = stream.select("value").as[String].map(Helper.toJsonWithId)

    documents.createOrReplaceTempView("raw_data")
    spark.udf.register("valueIfInLastXDays", Helper.valueIfInLastXDays)

    saveTransformed(spark,argsConfiguration)

  }

  private def saveTransformed(spark: SparkSession, argsConfiguration: Configuration): Unit = {

    val processed = spark.sql("SELECT VIN AS `vin`, first(make) AS `make`, first(`year`) AS `year`, max(valueIfInLastXDays(speed, hrTimestamp, 1)) as `maxSpeedToday`, max(valueIfInLastXDays(speed, hrTimestamp, 7)) as `maxSpeedLast7Days`, avg(cast(`speed` AS Double)) AS `avgSpeed`, max(cast(`instantFuelEconomy` AS Double)) AS `bestFuelEconomy`, avg(cast(`instantFuelEconomy` AS Double)) AS `totalFuelEconomy`, cast(count(vin) as Int) as `dataPointCount`, sum(speed) as `speedSum`, max(odometer) as `odometer`, min(cast(valueIfInLastXDays(odometer, hrTimestamp, 1) as Double)) as `minOdometerToday`, max(cast(valueIfInLastXDays(odometer, hrTimestamp, 1) as Double)) as `maxOdometerToday`, min(cast(valueIfInLastXDays(odometer, hrTimestamp, 7) as Double)) as `minOdometerThisWeek`, max(cast(valueIfInLastXDays(odometer, hrTimestamp, 7) AS Double)) as `maxOdometerThisWeek` FROM raw_data GROUP BY vin")


    val query = processed.writeStream.format(MapRDBSourceConfig.Format)
      .option(MapRDBSourceConfig.TablePathOption, argsConfiguration.transformed)
      .option(MapRDBSourceConfig.CreateTableOption,false)
      .option(MapRDBSourceConfig.IdFieldPathOption, "vin")
      .option("checkpointLocation", argsConfiguration.checkpoint)
      .outputMode("complete")
      .start()

    query.awaitTermination()
  }


}
