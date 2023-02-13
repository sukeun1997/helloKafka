package com.example.hellokafka.consumer

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest
class SimpleConsumerTest {

    @Autowired
    private lateinit var simpleConsumer: SimpleConsumer

    @Autowired
    private lateinit var kafkaConsumer: KafkaConsumer<String, String>

    @Test
    fun `동기 컨슈머 테스트`() {
        simpleConsumer.consume()
    }

    @Test
    fun `비동기 컨슈머 테스트`() {
        simpleConsumer.consumeAsync()
    }

    @Test
    fun `커스텀 리밸런스 컨슈머 테스트`() {
        simpleConsumer.consumeWithRebalanceListener()
    }

    @Test
    fun `컨슈머 안전한 종료 테스트`() {
        Runtime.getRuntime().addShutdownHook(Thread {
            kafkaConsumer.wakeup()
        })

        simpleConsumer.consume()
    }


    @Test
    fun `컨슈머 Metrics 테스트`() {
        kafkaConsumer.metrics().forEach { (key, value) ->
            println("${key.name()}: ${value.metricValue()}")
        }

    }
}