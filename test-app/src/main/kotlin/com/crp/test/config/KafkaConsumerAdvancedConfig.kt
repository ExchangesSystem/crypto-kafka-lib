package com.crp.test.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties

@Configuration
class KafkaConsumerAdvancedConfig {
    
    @Value("\${kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String
    
    @Value("\${kafka.consumer.group-id:test-consumer-group}")
    private lateinit var groupId: String
    
    // Manual acknowledgment configuration
    @Bean
    fun kafkaManualAckListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = consumerFactoryWithManualAck()
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL
        return factory
    }
    
    @Bean
    fun consumerFactoryWithManualAck(): ConsumerFactory<String, String> {
        val props = HashMap<String, Any>()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ConsumerConfig.GROUP_ID_CONFIG] = "$groupId-manual"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        return DefaultKafkaConsumerFactory(props)
    }
    
    // Batch listener configuration
    @Bean
    fun kafkaBatchListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = consumerFactoryForBatch()
        factory.isBatchListener = true
        factory.containerProperties.ackMode = ContainerProperties.AckMode.BATCH
        return factory
    }
    
    @Bean
    fun consumerFactoryForBatch(): ConsumerFactory<String, String> {
        val props = HashMap<String, Any>()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ConsumerConfig.GROUP_ID_CONFIG] = "$groupId-batch"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 10 // Process 10 messages at a time
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        return DefaultKafkaConsumerFactory(props)
    }
}