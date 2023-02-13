package com.example.hellokafka.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.event.annotation.AfterTestClass

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SimpleProducerTest {

    @Autowired
    private lateinit var simpleProducer: SimpleProducer

    @Autowired
    private lateinit var kafkaProducer: KafkaProducer<String, String>

    private val partitionNo = 1
    private val topicName = "test1"

    @Test
    fun `키 없이 값만 전송`() {
        val record = ProducerRecord<String, String>(topicName, "a", "hello")
        simpleProducer.sendMessage(record)
    }

    @Test
    fun `키 와 값 전송`() {
        val record = ProducerRecord(topicName, "key1", "hello")
        simpleProducer.sendMessage(record)
    }

    @Test
    fun `파티션 번호를 지정한 키와 값 전송`() {
        val record = ProducerRecord(topicName, partitionNo, "key1", "hello")
        simpleProducer.sendMessage(record)
    }

    @Test
    fun `커스텀 파티셔너를 사용하는 키와 값 전송`() {
        val record = ProducerRecord(topicName, "key1", "hello")
        simpleProducer.sendMessage(record)
    }

    @AfterAll
    fun `프로듀서 종료`() {
        kafkaProducer.flush()
        kafkaProducer.close()
    }
}