package configurations

import java.util.Properties
import com.typesafe.config.Config
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.OutputTag

class BaseConfiguration(val config:Config) extends Serializable {

  val obsdatatopic:String = config.getString("obsdata.topic")
  val obsdatatopic1:String = config.getString("obsdata.topic1")
  val transformedOutputTag = OutputTag[String]("transformed-output")

  def flinkKafkaProperties:Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "consumerGroup")
    properties
  }

}
