package com.syngenta.flink.transformer.functions

import com.syngenta.flink.transformer.caseclasses.ObsData
import com.syngenta.flink.transformer.configurations.ObsDataTransformerConfig
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

class ObsDataProcessFunction(config: ObsDataTransformerConfig) extends ProcessFunction[String, String] {

  lazy val state: ValueState[ObsData] = getRuntimeContext.getState(new ValueStateDescriptor[ObsData]("myState", classOf[ObsData]))


  override def processElement(value: String,
                              ctx: ProcessFunction[String, String]#Context,
                              out: Collector[String]): Unit = {


    val obsDataTransformed = new ObsDataTransformer

    out.collect(obsDataTransformed.obsTransform(value))
    ctx.output(config.transformedOutputTag, String.valueOf(obsDataTransformed.obsTransform(value)))


  }

}
