package com.example.hellokafka.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.stereotype.Component

private val logger = mu.KotlinLogging.logger {}

@Component
class SimpleProducer(
    private val producer: KafkaProducer<String, String>
) {
    fun sendMessage(record: ProducerRecord<String, String>) {
        producer.send(record) { metadata, exception ->
            if (exception != null) {
                logger.error("Error while producing", exception)
            } else {
                logger.info("Message sent to topic: ${metadata.topic()}, partition: ${metadata.partition()}, offset: ${metadata.offset()}")
            }
        }
    }
}