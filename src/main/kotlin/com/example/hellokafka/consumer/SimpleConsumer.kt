package com.example.hellokafka.consumer

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.springframework.stereotype.Component
import java.time.Duration
import kotlin.math.log

private val logger = mu.KotlinLogging.logger {}

private const val TOPIC_NAME = "test1"

@Component
class SimpleConsumer(
    private val consumer: KafkaConsumer<String, String>
) {

    fun consume() {

        consumer.subscribe(listOf(TOPIC_NAME))

        try {
            while (true) {
                val records = consumer.poll(Duration.ofSeconds(1))
                val currentOffset = hashMapOf<TopicPartition, OffsetAndMetadata>()
                records.forEach { record ->
                    val topicPartition = TopicPartition(record.topic(), record.partition())
                    val offsetAndMetadata = OffsetAndMetadata(record.offset() + 1)
                    currentOffset[topicPartition] = offsetAndMetadata
                    println("Received message: (${record.key()}, ${record.value()}) at offset ${record.offset()}")
                    consumer.commitSync(currentOffset)
                }
            }
        } catch (e: WakeupException) {
            logger.info("Consumer is closed")
        } catch (e: Exception) {
            logger.error("Error while consuming", e)
        } finally {
            consumer.close()
        }
    }

    fun consumeAsync() {
        consumer.subscribe(listOf(TOPIC_NAME))
        while (true) {
            val records = consumer.poll(Duration.ofSeconds(1))
            records.forEach { record ->
                logger.info("Received message: (${record.key()}, ${record.value()}) at offset ${record.offset()}")
            }
            consumer.commitAsync { offsets, exception ->
                if (exception != null) {
                    println("Commit failed for offsets $offsets")
                }

            }
        }
    }

    fun consumeWithRebalanceListener() {
        consumer.subscribe(listOf(TOPIC_NAME), CustomConsumerRebalanceListener())
        while (true) {
            val records = consumer.poll(Duration.ofSeconds(1))
            records.forEach { record ->
                logger.info("Received message: (${record.key()}, ${record.value()}) at offset ${record.offset()}")
            }
            consumer.commitAsync { offsets, exception ->
                if (exception != null) {
                    println("Commit failed for offsets $offsets")
                }

            }
        }
    }

    private val partitionNumber = 0

    fun consumeWithSpecificPartition() {
        val topicPartition = TopicPartition(TOPIC_NAME, partitionNumber)
        consumer.assign(listOf(topicPartition))
        while (true) {
            val records = consumer.poll(Duration.ofSeconds(1))
            records.forEach { record ->
                logger.info("Received message: (${record.key()}, ${record.value()}) at offset ${record.offset()}")
            }
            consumer.commitAsync { offsets, exception ->
                if (exception != null) {
                    println("Commit failed for offsets $offsets")
                }

            }
        }
    }
}

class CustomConsumerRebalanceListener : ConsumerRebalanceListener {
    override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>?) {
        logger.info("Called onPartitionsRevoked with partitions $partitions")
    }

    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>?) {
        logger.info("Called onPartitionsAssigned with partitions $partitions")
    }
}