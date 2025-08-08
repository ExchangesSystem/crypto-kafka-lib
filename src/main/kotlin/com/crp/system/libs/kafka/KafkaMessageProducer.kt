package com.crp.system.libs.kafka

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import org.springframework.kafka.core.KafkaTemplate;

@Component
data class KafkaMessageProducer(
    @Autowired
    var kafkaTemplate: KafkaTemplate<String, String>) {

    fun sendMessage(topic: String, message: String) {
        kafkaTemplate.send(topic, message)
    }
}