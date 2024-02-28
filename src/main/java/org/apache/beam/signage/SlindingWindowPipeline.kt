package org.apache.beam.signage

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.transforms.windowing.SlidingWindows
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.TypeDescriptors
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.joda.time.DateTimeZone
import org.joda.time.Duration
import org.joda.time.Instant

fun main(args: Array<String>) {
    val p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(*args).withValidation().create()
    )

    val addr = "localhost:9092"
    val kafkaInput = p.apply(
        KafkaIO.read<Long, String>()
            .withBootstrapServers(addr)
            .withTopic("weather-info")
            .withKeyDeserializer(LongDeserializer::class.java)
            .withValueDeserializer(StringDeserializer::class.java)
            .withTimestampPolicyFactory(WeatherTimestampPolicyFactory())
            .withReadCommitted()
            .withoutMetadata())

    val weather = kafkaInput.apply(
        ParDo.of(object : DoFn<KV<Long, String>, Weather>() {
            @ProcessElement
            fun processElement(c: ProcessContext) {
                val objectMapper = jacksonObjectMapper()
                val weather = objectMapper.readValue<Weather>(c.element().value)
                c.output(weather)
            }
        }))

    val temperature = weather.apply(
        MapElements
            .into(TypeDescriptors.floats())
            .via(SerializableFunction { weather -> weather.temperature })
    )

    val windowedTemperature = temperature.apply(
        Window.into<Float>(
            SlidingWindows
                .of(Duration.standardHours(3))
                .every(Duration.standardHours(1))
        )
    )

    val windowedTemperatureWithDate = windowedTemperature.apply(
        ParDo.of(object : DoFn<Float, KV<Instant, Float>>() {
            @ProcessElement
            fun processElement(c: ProcessContext, window: IntervalWindow) {
                c.output(KV.of(window.start(), c.element()))
            }
        })
    )

    val meanTemperature = windowedTemperatureWithDate.apply(
        Mean.perKey<Instant, Float>()
    )

    val meanTemperatureLine = meanTemperature.apply(
        MapElements.into(TypeDescriptors.strings()).via(
            SerializableFunction { mean ->
                "most old datetime: ${mean.key.toDateTime(DateTimeZone.forID("+09:00"))} \tmeanTemperature(per 3-hour): ${mean.value}"
            }
        )
    )

    meanTemperatureLine.apply(
        KafkaIO.write<Void, String>()
            .withBootstrapServers(addr)
            .withTopic("beam-out")
            .withValueSerializer(StringSerializer::class.java)
            .values()
    )

    p.run().waitUntilFinish()

}