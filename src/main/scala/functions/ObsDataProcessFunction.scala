package functions

import caseclasses.{Agg_Method, Agg_Time_Window, ObsData, ObsDataTransform}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import configurations.BaseConfiguration
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

class ObsDataProcessFunction(config: BaseConfiguration) extends ProcessFunction[String, String] {

  lazy val state: ValueState[ObsData] = getRuntimeContext.getState(new ValueStateDescriptor[ObsData]("myState", classOf[ObsData]))


  override def processElement(value: String,
                              ctx: ProcessFunction[String, String]#Context,
                              out: Collector[String]): Unit = {

    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val obsData: ObsData = objectMapper.readValue[ObsData](value)
    val output = new ByteArrayOutputStream()


    val agg_Time_Window = Agg_Time_Window(obsData.codeComponents(0).componentCode, obsData.codeComponents(0).selector, obsData.codeComponents(0).value, obsData.codeComponents(0).valueUoM)
    val agg_Method = Agg_Method(obsData.codeComponents(1).componentCode, obsData.codeComponents(1).selector, obsData.codeComponents(1).value, obsData.codeComponents(1).valueUoM)

    val obsDataTransform: ObsDataTransform = ObsDataTransform(obsData.obsCode, agg_Time_Window, agg_Method, obsData.valueUoM, obsData.value, obsData.id, obsData.parentCollectionRef, obsData.integrationAccountRef, obsData.assetRef, obsData.xMin, obsData.xMax, obsData.yMin, obsData.yMax, obsData.phenTime, obsData.spatialExtent)

    objectMapper.writeValue(output, obsDataTransform)


    out.collect(output.toString)
    ctx.output(config.transformedOutputTag, String.valueOf(output))


  }

}
