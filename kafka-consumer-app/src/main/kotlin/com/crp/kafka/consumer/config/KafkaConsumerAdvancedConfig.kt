package com.crp.kafka.consumer.config

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
    
    @Value("\${kafka.consumer.group-id:crypto-consumer-group}")
    private lateinit var groupId: String
    
    /**
     * Manual acknowledgment consumer factory and listener
     */
    @Bean
    fun kafkaManualAckListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = consumerFactoryWithManualAck()
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL
        factory.containerProperties.syncCommits = true
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
        props[ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG] = 30000
        props[ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG] = 3000
        props[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG] = 300000
        return DefaultKafkaConsumerFactory(props)
    }
    
    /**
     * Batch processing consumer factory and listener
     */
    @Bean
    fun kafkaBatchListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = consumerFactoryForBatch()
        factory.isBatchListener = true
        factory.containerProperties.ackMode = ContainerProperties.AckMode.BATCH
        factory.setConcurrency(2) // Run 2 consumer threads for batch processing
        return factory
    }
    
    @Bean
    fun consumerFactoryForBatch(): ConsumerFactory<String, String> {
        val props = HashMap<String, Any>()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ConsumerConfig.GROUP_ID_CONFIG] = "$groupId-batch"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 50 // Process 50 messages per batch
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        props[ConsumerConfig.FETCH_MIN_BYTES_CONFIG] = 1024 // Wait for at least 1KB of data
        props[ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG] = 1000 // Wait maximum 1 second
        return DefaultKafkaConsumerFactory(props)
    }
    
    /**
     * High throughput consumer factory for real-time processing
     */
    @Bean
    fun kafkaHighThroughputListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = consumerFactoryHighThroughput()
        factory.setConcurrency(4) // Run 4 consumer threads for high throughput
        factory.containerProperties.pollTimeout = 1000
        return factory
    }
    
    @Bean
    fun consumerFactoryHighThroughput(): ConsumerFactory<String, String> {
        val props = HashMap<String, Any>()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ConsumerConfig.GROUP_ID_CONFIG] = "$groupId-realtime"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest" // Only process new messages
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 1000 // High throughput
        props[ConsumerConfig.FETCH_MIN_BYTES_CONFIG] = 50000 // 50KB minimum fetch size
        props[ConsumerConfig.FETCH_MAX_BYTES_CONFIG] = 52428800 // 50MB maximum fetch size
        props[ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG] = 10000 // Shorter session timeout
        props[ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG] = 3000
        return DefaultKafkaConsumerFactory(props)
    }
    
    /**
     * Error handling consumer factory
     */
    @Bean
    fun kafkaErrorHandlingListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = consumerFactoryForErrorHandling()
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL
        factory.setConcurrency(1) // Single thread for error processing
        return factory
    }
    
    @Bean
    fun consumerFactoryForErrorHandling(): ConsumerFactory<String, String> {
        val props = HashMap<String, Any>()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ConsumerConfig.GROUP_ID_CONFIG] = "$groupId-errors"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 1 // Process errors one by one
        props[ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG] = 60000 // Longer timeout for error processing
        return DefaultKafkaConsumerFactory(props)
    }
}