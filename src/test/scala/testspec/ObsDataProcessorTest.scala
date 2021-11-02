package testspec

import caseclasses.ObsData
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.config.{Config, ConfigFactory}
import configurations.{BaseConfiguration, KafkaConnector}
import data.TestData
import flinkjobs.ObsDataProcessor
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.mockito.MockitoSugar.mock
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayOutputStream
import java.util

class ObsDataProcessorTest extends AnyFlatSpec with Matchers{

  val config: Config = ConfigFactory.load("obsconfig.conf").getConfig("com.obs.batch")
  val baseConfiguration = new BaseConfiguration(config)

  val mockKafkaConnector: KafkaConnector = mock[KafkaConnector](Mockito.withSettings().serializable())


  when(mockKafkaConnector.kafkaConsumer(baseConfiguration.obsdatatopic))thenReturn(new FlinkEventSource)

  when(mockKafkaConnector.kafkaProducer(baseConfiguration.obsdatatopic1))thenReturn(new ObsDataSink)



  " Obs Data Processor " should "process the data" in{

    val task = new ObsDataProcessor(baseConfiguration, mockKafkaConnector)
    task.process()

    ObsDataSink.values.size() should be(2)



  }



}

class FlinkEventSource extends SourceFunction[String]{
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {


    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)

    val data1: ObsData = objectMapper.readValue[ObsData](TestData.Data_1)
    val testData1 = new ByteArrayOutputStream()

    objectMapper.writeValue(testData1, data1)

    ctx.collect(testData1.toString)

    val data2: ObsData = objectMapper.readValue[ObsData](TestData.Data_2)
    val testData2 = new ByteArrayOutputStream()
    objectMapper.writeValue(testData2, data2)




    ctx.collect(testData2.toString)



  }

  override def cancel(): Unit = {}

}
class ObsDataSink extends SinkFunction[String]{
  override def invoke(value: String, context: SinkFunction.Context): Unit = {
    synchronized{
      ObsDataSink.values.add(value)
    }
  }
}
object ObsDataSink{
  val values: util.List[String] = new util.ArrayList()
}