package com.mpojeda84.mapr.scala.config

case class Configuration(checkpoint: String, topic: String, transformed: String, dateOffset: String)

object Configuration {

  def parse(args: Seq[String]): Configuration = parser.parse(args, Configuration.default).get

  def default: Configuration = DefaultConfiguration

  object DefaultConfiguration extends Configuration(
    "path/to/json",
    "/path/to/stream:topic",
    "/obd/car-data-transformed",
    "2019-01-28 0:17:08"
  )

  private val parser = new scopt.OptionParser[Configuration]("App Name") {
    head("App Name")

    opt[String]('h', "checkpoint")
      .action((t, config) => config.copy(checkpoint = t))
      .maxOccurs(1)
      .text("Checkpoint Location")

    opt[String]('r', "transformed")
      .action((t, config) => config.copy(transformed = t))
      .maxOccurs(1)
      .text("MapR-DB table name to write results to")

    opt[String]('n', "topic")
      .action((s, config) => config.copy(topic = s))
      .text("Topic where Kafka Producer is writing to")

    opt[String]('o', "dateOffset")
      .optional()
      .action((s, config) => config.copy(dateOffset = s))
      .text("Offset Date, default: '2019-01-28 0:17:08'")

  }
}