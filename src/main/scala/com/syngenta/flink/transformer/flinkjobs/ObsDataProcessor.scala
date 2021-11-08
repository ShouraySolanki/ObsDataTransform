package com.syngenta.flink.transformer.flinkjobs

import com.typesafe.config.ConfigFactory
import com.syngenta.flink.transformer.configurations.ObsDataTransformerConfig
import com.syngenta.flink.transformer.functions.ObsDataProcessFunction
import org.apache.flink.streaming.api.scala._

class ObsDataProcessor(config: ObsDataTransformerConfig, kafkaConnector: KafkaConnector) {
  def process(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val obsDataProcessFunction = new ObsDataProcessFunction(config)


    val stream = env.addSource(kafkaConnector.kafkaConsumer(config.obsdatatopic))

      .process(obsDataProcessFunction)


    stream.getSideOutput(config.transformedOutputTag).addSink(kafkaConnector.kafkaProducer(config.obsdatatopic1))

    env.execute()

  }
}

object ObsDataProcessor {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("obsconfig.conf")
    val baseConfiguration = new ObsDataTransformerConfig(config)
    val kafkaConnector = new KafkaConnector(baseConfiguration)
    val task = new ObsDataProcessor(baseConfiguration, kafkaConnector)
    task.process()
  }
}