package org.apache.beam.signage

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer

fun main(args: Array<String>) {
    val options = PipelineOptionsFactory.fromArgs(*args).withValidation().create()
    val pipeline = Pipeline.create(options)
    val addr = "localhost:9092"

    val kafkaRead = KafkaIO.read<Long, String>()
        .withBootstrapServers(addr)
        .withTopic("weather-info")
        .withKeyDeserializer(LongDeserializer::class.java)
        .withValueDeserializer(StringDeserializer::class.java)
        .withReadCommitted()
        .withoutMetadata()

    val kafkaWrite = KafkaIO.write<Long, String>()
        .withBootstrapServers(addr)
        .withTopic("beam-out")
        .withKeySerializer(LongSerializer::class.java)
        .withValueSerializer(StringSerializer::class.java)

    pipeline.apply("ReadFromKafka", kafkaRead)
        .apply("WriteToKafka", kafkaWrite)

    pipeline.run().waitUntilFinish()
}