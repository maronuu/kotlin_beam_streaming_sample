package org.apache.beam.signage

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.beam.sdk.io.kafka.KafkaRecord
import org.apache.beam.sdk.io.kafka.TimestampPolicy
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory
import org.apache.kafka.common.TopicPartition
import org.joda.time.Instant
import java.util.Optional

class WeatherTimestampPolicyFactory<K> : TimestampPolicyFactory<K, String> {
    companion object {
        private val objectMapper = jacksonObjectMapper()
    }

    override fun createTimestampPolicy(
        tp: TopicPartition, previousWatermark: Optional<Instant>
    ): TimestampPolicy<K, String> = object : TimestampPolicy<K, String>() {
        var lastTimestamp: Instant = Instant.EPOCH

        override fun getTimestampForRecord(ctx: PartitionContext, rec: KafkaRecord<K, String>): Instant {
            val jsonWeather = rec.kv.value
            val weather: Weather = try {
                objectMapper.readValue(jsonWeather)
            } catch (e: Exception) {
                e.printStackTrace()
                return Instant.EPOCH
            }
            lastTimestamp = Instant.parse(weather.timestamp)
            return lastTimestamp
        }

        override fun getWatermark(ctx: PartitionContext): Instant {
            val prevWatermark = previousWatermark.orElse(Instant.EPOCH)
            // watermark is monotonically increasing
            return if (lastTimestamp.isAfter(prevWatermark)) lastTimestamp else prevWatermark
        }
    }
}
