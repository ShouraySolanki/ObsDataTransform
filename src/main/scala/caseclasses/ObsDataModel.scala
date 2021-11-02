package caseclasses

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.google.gson.annotations.SerializedName


case class ObsData( obsCode: String,
                    @(SerializedName @scala.annotation.meta.field)("agg_time_window")codeComponents: List[CodeComponents],
                    valueUoM: String,
                    value: String,
                    id: String,
                    parentCollectionRef: String,
                    integrationAccountRef:String,
                    assetRef: String,
                    xMin: Double,
                    xMax: Double,
                    yMin: Double,
                    yMax: Double,
                    phenTime: String,
                    spatialExtent: String
                  )

case class  CodeComponents( componentCode: String,
                            componentType: String,
                            selector: String,
                            value: Option[String] = None,
                            valueUoM: Option[String] = None
                          )

case class ObsDataTransform(  obsCode: String,
                              agg_Time_Window: Agg_Time_Window,
                              agg_Method: Agg_Method,
                              valueUoM: String,
                              value: String,
                              id: String,
                              parentCollectionRef: String,
                              integrationAccountRef:String,
                              assetRef: String,
                              xMin: Double,
                              xMax: Double,
                              yMin: Double,
                              yMax: Double,
                              phenTime: String,
                              spatialExtent: String
                            )
@JsonIgnoreProperties(ignoreUnknown = true)
case class  Agg_Time_Window(@JsonProperty("componentCode")componentCode: String,
                            @JsonProperty("selector")selector: String,
                            @JsonProperty("value")value: Option[String] = None,
                            @JsonProperty("valueUoM")valueUoM: Option[String] = None
                          )
case class  Agg_Method(@JsonProperty("componentCode")componentCode: String,
                       @JsonProperty("selector")selector: String,
                       @JsonProperty("value")value: Option[String] = None,
                       @JsonProperty("valueUoM")valueUoM: Option[String] = None
                      )