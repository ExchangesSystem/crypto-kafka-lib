package com.crp.kafka.shared.constants

object KafkaConstants {
    
    // Default Topics
    object Topics {
        const val DEFAULT = "messages"
        const val USER_EVENTS = "user-events"
        const val TRANSACTION_EVENTS = "transaction-events"
        const val NOTIFICATIONS = "notifications"
        const val AUDIT_LOGS = "audit-logs"
        const val ERROR_EVENTS = "error-events"
        const val DEAD_LETTER_QUEUE = "dlq"
        
        // High throughput topics
        const val BATCH_PROCESSING = "batch-processing"
        const val BULK_OPERATIONS = "bulk-operations"
        
        // Real-time topics
        const val REAL_TIME_EVENTS = "realtime-events"
        const val WEBSOCKET_MESSAGES = "websocket-messages"
        
        // Monitoring topics
        const val METRICS = "metrics"
        const val HEALTH_CHECKS = "health-checks"
    }
    
    // Consumer Groups
    object ConsumerGroups {
        const val DEFAULT = "default-consumer-group"
        const val USER_SERVICE = "user-service-consumers"
        const val TRANSACTION_SERVICE = "transaction-service-consumers"
        const val NOTIFICATION_SERVICE = "notification-service-consumers"
        const val AUDIT_SERVICE = "audit-service-consumers"
        const val MONITORING_SERVICE = "monitoring-service-consumers"
        
        // Processing patterns
        const val BATCH_PROCESSOR = "batch-processor-group"
        const val REAL_TIME_PROCESSOR = "realtime-processor-group"
        const val ERROR_HANDLER = "error-handler-group"
    }
    
    // Headers
    object Headers {
        const val MESSAGE_ID = "X-Message-ID"
        const val CORRELATION_ID = "X-Correlation-ID"
        const val USER_ID = "X-User-ID"
        const val SESSION_ID = "X-Session-ID"
        const val TIMESTAMP = "X-Timestamp"
        const val SOURCE_SERVICE = "X-Source-Service"
        const val MESSAGE_TYPE = "X-Message-Type"
        const val RETRY_COUNT = "X-Retry-Count"
        const val TRACE_ID = "X-Trace-ID"
        const val SPAN_ID = "X-Span-ID"
    }
    
    // Configuration Keys
    object Config {
        const val BOOTSTRAP_SERVERS = "kafka.bootstrap-servers"
        const val CONSUMER_GROUP_ID = "kafka.consumer.group-id"
        const val PRODUCER_CLIENT_ID = "kafka.producer.client-id"
        const val SECURITY_PROTOCOL = "kafka.security.protocol"
        const val RETRIES = "kafka.producer.retries"
        const val BATCH_SIZE = "kafka.producer.batch-size"
        const val LINGER_MS = "kafka.producer.linger-ms"
        const val BUFFER_MEMORY = "kafka.producer.buffer-memory"
        const val AUTO_OFFSET_RESET = "kafka.consumer.auto-offset-reset"
        const val ENABLE_AUTO_COMMIT = "kafka.consumer.enable-auto-commit"
        const val MAX_POLL_RECORDS = "kafka.consumer.max-poll-records"
    }
    
    // Default Values
    object Defaults {
        const val RETRIES = 3
        const val BATCH_SIZE = 16384
        const val LINGER_MS = 1L
        const val BUFFER_MEMORY = 33554432L
        const val MAX_POLL_RECORDS = 500
        const val SESSION_TIMEOUT_MS = 30000
        const val REQUEST_TIMEOUT_MS = 60000
        const val AUTO_OFFSET_RESET = "earliest"
        const val ENABLE_AUTO_COMMIT = true
        const val AUTO_COMMIT_INTERVAL_MS = 1000
    }
    
    // Message Versioning
    object Versions {
        const val V1_0 = "1.0"
        const val V1_1 = "1.1"
        const val CURRENT = V1_1
    }
    
    // Error Types
    object ErrorTypes {
        const val SERIALIZATION_ERROR = "SERIALIZATION_ERROR"
        const val DESERIALIZATION_ERROR = "DESERIALIZATION_ERROR"
        const val VALIDATION_ERROR = "VALIDATION_ERROR"
        const val PROCESSING_ERROR = "PROCESSING_ERROR"
        const val TIMEOUT_ERROR = "TIMEOUT_ERROR"
        const val CONNECTION_ERROR = "CONNECTION_ERROR"
        const val AUTHENTICATION_ERROR = "AUTHENTICATION_ERROR"
        const val AUTHORIZATION_ERROR = "AUTHORIZATION_ERROR"
    }
}