package org.apache.beam.signage

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.beam.sdk.schemas.JavaFieldSchema
import org.apache.beam.sdk.schemas.annotations.DefaultSchema
import org.apache.beam.sdk.schemas.annotations.SchemaCreate

@DefaultSchema(JavaFieldSchema::class)
data class Weather @JsonCreator @SchemaCreate constructor(
    @JsonProperty("timestamp") val timestamp: String,
    @JsonProperty("temperature [°C]") val temperature: Float,
    @JsonProperty("rainfall [mm]") val rainfall: Float
) {
    fun toLine(): String = "$timestamp\ttemperature:$temperature\trainfall:$rainfall"
}
