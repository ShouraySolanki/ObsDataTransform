package com.syngenta.flink.transformer.flinkjobs

import com.typesafe.config.ConfigFactory
import com.syngenta.flink.transformer.configurations.{BaseConfiguration, KafkaConnector}
import com.syngenta.flink.transformer.functions.ObsDataProcessFunction
import org.apache.flink.streaming.api.scala._

class ObsDataProcessor(config: BaseConfiguration, kafkaConnector: KafkaConnector) {
  def process(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val obsDataProcessFunction = new ObsDataProcessFunction(config)


    val stream = env.addSource(kafkaConnector.kafkaConsumer(config.obsdatatopic))

      .process(obsDataProcessFunction)


    stream.getSideOutput(config.transformedOutputTag).addSink(kafkaConnector.kafkaProducer(config.obsdatatopic1))

    env.execute()

  }
}

object ObsDataProcessor{
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("obsconfig.conf").getConfig("com.obs.batch")
    val baseConfiguration = new BaseConfiguration(config)
    val kafkaConnector = new KafkaConnector(baseConfiguration)
    val task = new ObsDataProcessor(baseConfiguration, kafkaConnector)
    task.process()
  }
}