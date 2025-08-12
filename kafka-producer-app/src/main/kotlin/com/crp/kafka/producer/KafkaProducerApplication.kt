package com.crp.kafka.producer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.ComponentScan

@SpringBootApplication
@ComponentScan(
    basePackages = [
        "com.crp.kafka.producer",
        "com.crp.system.libs.kafka"
    ]
)
class KafkaProducerApplication

fun main(args: Array<String>) {
    runApplication<KafkaProducerApplication>(*args)
}