package com.crp.test.consumer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.ComponentScan
import org.springframework.kafka.annotation.EnableKafka

@SpringBootApplication
@EnableKafka
@ComponentScan(basePackages = ["com.crp.test.consumer", "com.crp.system.libs.kafka"])
class KafkaConsumerApplication

fun main(args: Array<String>) {
    runApplication<KafkaConsumerApplication>(*args)
}