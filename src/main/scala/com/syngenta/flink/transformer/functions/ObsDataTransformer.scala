package com.syngenta.flink.transformer.functions

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import com.syngenta.flink.transformer.domain.{ComponentType, ObsData}
import org.apache.commons.text.CaseUtils
import org.json.JSONObject

import java.util

class ObsDataTransformer {
  def obsTransform(value: String): String = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val obsData: ObsData = objectMapper.readValue[ObsData](value)
    val spatialExtentJsnObj: JSONObject = new JSONObject(obsData.spatialExtent)

    val transformed: util.HashMap[String, Any] = new util.HashMap[String, Any]()
    transformed.put("obsCode", obsData.obsCode)
    obsData.codeComponents.foreach(components =>
      transformed.put(CaseUtils.toCamelCase(components.componentType, false, '_'), ComponentType(components.componentCode, components.selector, components.value, components.valueUoM)))
    transformed.put("valueUoM", obsData.valueUoM)
    transformed.put("value", obsData.value)
    transformed.put("id", obsData.id)
    transformed.put("parentCollectionRef", List(obsData.parentCollectionRef))
    transformed.put("integrationAccountRef", obsData.integrationAccountRef)
    transformed.put("assetRef", obsData.assetRef)
    transformed.put("xMin", obsData.xMin)
    transformed.put("xMax", obsData.xMax)
    transformed.put("yMin", obsData.yMin)
    transformed.put("yMax", obsData.yMax)
    transformed.put("phenTime", obsData.phenTime)
    transformed.put("spatialExtent", Map("type" -> spatialExtentJsnObj.getString("type"), "latCoordinates" -> spatialExtentJsnObj.getJSONArray("coordinates").get(0), "lonCoordinates" -> spatialExtentJsnObj.getJSONArray("coordinates").get(1))) 

    val obsTransformed: String = objectMapper.writeValueAsString(transformed)

    obsTransformed

  }

}
