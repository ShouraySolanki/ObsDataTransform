package com.syngenta.flink.transformer.configurations

import com.syngenta.flink.transformer.caseclasses.{ComponentTypeOne, ComponentTypeTwo, ObsData}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import com.typesafe.config.Config
import org.apache.commons.text.CaseUtils
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.OutputTag
import org.json.JSONObject

import java.util
import java.util.Properties

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

  def obsTransform(value:String):String = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val obsData: ObsData = objectMapper.readValue[ObsData](value)
    val jsnobj: JSONObject = new JSONObject(obsData.spatialExtent)




    val transformed: util.HashMap[String, Any] =  new util.HashMap[String, Any]()

    transformed.put("obsCode", obsData.obsCode)
    transformed.put(CaseUtils.toCamelCase(obsData.codeComponents(0).componentType, false,  '_'), ComponentTypeOne(obsData.codeComponents(0).componentCode, obsData.codeComponents(0).selector, obsData.codeComponents(0).value, obsData.codeComponents(0).valueUoM) )
    transformed.put(CaseUtils.toCamelCase(obsData.codeComponents(1).componentType,false,'_'), ComponentTypeTwo(obsData.codeComponents(1).componentCode,obsData.codeComponents(1).selector,obsData.codeComponents(1).value, obsData.codeComponents(1).valueUoM))
    transformed.put("valueUoM", obsData.valueUoM)
    transformed.put("value", obsData.value)
    transformed.put("id", obsData.id)
    transformed.put("parentCollectionRef", obsData.parentCollectionRef)
    transformed.put("integrationAccountRef", obsData.integrationAccountRef)
    transformed.put("assetRef", obsData.assetRef)
    transformed.put("xMin", obsData.xMin)
    transformed.put("xMax", obsData.xMax)
    transformed.put("yMin", obsData.yMin)
    transformed.put("yMax", obsData.yMax)
    transformed.put("phenTime", obsData.phenTime)
    transformed.put("spatialExtent", Map("type"-> jsnobj.getString("Type"), "latCoordinates" -> jsnobj.getJSONArray("coordinates").get(0), "lonCoordinates" -> jsnobj.getJSONArray("coordinates").get(1)))//SpatialExtent(jsnobj.getString("Type"), jsnobj.getJSONArray("coordinates").get(0),jsnobj.getJSONArray("coordinates").get(1)  ))

    val obsTransformed:String= objectMapper.writeValueAsString(transformed)

    obsTransformed

  }

}
