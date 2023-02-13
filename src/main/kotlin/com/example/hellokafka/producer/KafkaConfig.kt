package com.example.hellokafka.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import java.util.*

@Configuration
class KafkaConfig {

    @Bean
    fun customKafkaProducer(): KafkaProducer<String, String> {
        return KafkaProducer<String, String>(properties())
    }

    private fun properties(): Properties {
        val props = Properties()
        props["bootstrap.servers"] = BOOTSTRAP_SERVERS
        props["key.serializer"] = StringSerializer::class.java
        props["value.serializer"] = StringSerializer::class.java
        props["partitioner.class"] = CustomPartitioner::class.java
        return props
    }

    companion object {
        private const val BOOTSTRAP_SERVERS = "my-kafka:9092"
    }

}

@Component
class CustomPartitioner : Partitioner {

    override fun configure(configs: MutableMap<String, *>?) {
    }

    override fun close() {
    }

    override fun partition(
        topic: String?,
        key: Any?,
        keyBytes: ByteArray?,
        value: Any?,
        valueBytes: ByteArray?,
        cluster: Cluster?
    ): Int {

        if (keyBytes == null) {
            throw IllegalArgumentException("Key is null")
        }

        if (key as String == "key1") {
            return 0
        } else {
            val partitions = cluster!!.partitionsForTopic(topic)
            return Math.abs(key.hashCode() % partitions.size)
        }
    }
}