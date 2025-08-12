# Crypto Kafka Library - Separated Applications

This documentation covers the separated microservices architecture with individual producer and consumer applications using **KRaft mode** (Kafka without Zookeeper).

## 🏗️ **Architecture Overview**

```
crypto-kafka-lib/
├── src/main/kotlin/               # Core Kafka library
├── kafka-shared/                  # Shared DTOs and constants
├── kafka-producer-app/           # Standalone producer application
├── kafka-consumer-app/           # Standalone consumer application
├── test-app/                     # Legacy monolithic test app (deprecated)
└── docker-compose.kafka-kraft.yml # KRaft Kafka infrastructure
```

### **Key Improvements**
- ✅ **KRaft Mode**: Kafka without Zookeeper for better performance and simplified ops
- ✅ **Microservices**: Separated producer and consumer for independent scaling
- ✅ **Shared DTOs**: Common data models in separate module
- ✅ **Docker Ready**: Individual Docker compositions for each service
- ✅ **Production Ready**: Comprehensive monitoring, health checks, and error handling

## 🚀 **Quick Start**

### 1. Start Kafka Infrastructure (KRaft Mode)
```bash
# Start Kafka with KRaft mode (no Zookeeper needed!)
docker-compose -f docker-compose.kafka-kraft.yml up -d

# Verify Kafka is running
docker-compose -f docker-compose.kafka-kraft.yml ps

# Check Kafka UI
open http://localhost:8090
```

### 2. Build All Applications
```bash
# Build the entire project including separated apps
./gradlew build

# Or build specific modules
./gradlew :kafka-shared:build
./gradlew :kafka-producer-app:build  
./gradlew :kafka-consumer-app:build
```

### 3. Run Applications

**Option A: Using Gradle (Development)**
```bash
# Terminal 1 - Start Producer App
./gradlew :kafka-producer-app:bootRun

# Terminal 2 - Start Consumer App  
./gradlew :kafka-consumer-app:bootRun
```

**Option B: Using Docker (Production-like)**
```bash
# Start producer app
cd kafka-producer-app
docker-compose up -d

# Start consumer app
cd ../kafka-consumer-app
docker-compose up -d
```

**Option C: Using JAR Files**
```bash
# Build JARs
./gradlew :kafka-producer-app:bootJar
./gradlew :kafka-consumer-app:bootJar

# Run producer
java -jar kafka-producer-app/build/libs/kafka-producer-app-1.0.0.jar

# Run consumer
java -jar kafka-consumer-app/build/libs/kafka-consumer-app-1.0.0.jar
```

## 📱 **Applications**

### **Producer App** (Port 8080)
- **REST API**: Send messages via HTTP endpoints
- **Multiple Patterns**: Fire-and-forget, acknowledgment, batch sending
- **Message Types**: Simple messages, user events, transaction events
- **Monitoring**: Health checks, metrics, Prometheus support

### **Consumer App** (Port 8082)  
- **Multiple Listeners**: Basic, manual ACK, batch processing
- **Message Processing**: User events, transactions, notifications, etc.
- **Error Handling**: Dead letter queues, retry logic, error metrics
- **Monitoring**: Processing statistics, health dashboards

### **Shared Module**
- **DTOs**: Common data transfer objects
- **Constants**: Topic names, consumer groups, headers
- **Validation**: Request/response validation annotations

## 🛠️ **API Endpoints**

### **Producer App** (http://localhost:8080)

#### Send Messages
```bash
# Simple message
curl -X POST http://localhost:8080/api/v1/messages/send \
  -H "Content-Type: application/json" \
  -d '{"message": "Hello KRaft Kafka!"}'

# Message with acknowledgment
curl -X POST http://localhost:8080/api/v1/messages/send-with-ack \
  -H "Content-Type: application/json" \
  -d '{"message": "Important message"}'

# User event
curl -X POST http://localhost:8080/api/v1/messages/user-events \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user123",
    "message": "User logged in",
    "metadata": {"ip": "192.168.1.1", "device": "mobile"}
  }'

# Transaction event
curl -X POST http://localhost:8080/api/v1/messages/transaction-events \
  -H "Content-Type: application/json" \
  -d '{
    "transactionId": "tx-12345",
    "userId": "user123",
    "eventType": "TRANSACTION_CREATED",
    "amount": "100.50",
    "currency": "USD",
    "status": "PENDING"
  }'

# Send to custom topic
curl -X POST http://localhost:8080/api/v1/messages/send-to-topic \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "custom-notifications",
    "message": "Custom notification",
    "key": "user-123",
    "headers": {"priority": "high"}
  }'

# Batch send
curl -X POST http://localhost:8080/api/v1/messages/batch-send \
  -H "Content-Type: application/json" \
  -d '[
    {"message": "Batch message 1"},
    {"message": "Batch message 2"},
    {"message": "Batch message 3"}
  ]'
```

#### Monitoring
```bash
# Health check
curl http://localhost:8080/api/v1/messages/health

# Producer statistics  
curl http://localhost:8080/api/v1/messages/stats

# Actuator endpoints
curl http://localhost:8081/actuator/health
curl http://localhost:8081/actuator/metrics
curl http://localhost:8081/actuator/prometheus
```

### **Consumer App** (http://localhost:8082)

#### Monitoring
```bash
# Consumer statistics
curl http://localhost:8082/api/v1/monitoring/stats

# Topic statistics
curl http://localhost:8082/api/v1/monitoring/topics/stats

# Get processed message
curl http://localhost:8082/api/v1/monitoring/message/{messageId}

# Health check
curl http://localhost:8082/api/v1/monitoring/health

# Detailed health
curl http://localhost:8082/api/v1/monitoring/health/detailed

# Clear cache
curl -X POST http://localhost:8082/api/v1/monitoring/cache/clear
```

## 🐳 **Docker Configuration**

### **KRaft Kafka Stack**
The `docker-compose.kafka-kraft.yml` provides:
- **Kafka KRaft**: Single-node Kafka without Zookeeper
- **Kafka UI**: Web interface on port 8090
- **Schema Registry**: For Avro/JSON schemas (optional)
- **Kafka Connect**: For data integration (optional)
- **KSQL**: For stream processing (optional)

### **Individual App Containers**
Each app has its own `docker-compose.yml`:
- **Producer**: Port 8080 (API) + 8081 (Management)
- **Consumer**: Port 8082 (API) + 8083 (Management)
- **Health Checks**: Automated health monitoring
- **Resource Limits**: Memory and CPU constraints

## 📊 **Monitoring & Observability**

### **Metrics Available**
- **Producer Metrics**: Messages sent, failure rate, throughput
- **Consumer Metrics**: Messages processed, lag, error rate
- **Kafka Metrics**: Broker health, topic statistics
- **JVM Metrics**: Memory, GC, threads

### **Health Checks**
- **Liveness**: Application is running
- **Readiness**: Application is ready to serve traffic
- **Custom**: Kafka connectivity, message processing health

### **Alerting Recommendations**
```yaml
# Example Prometheus alerts
- alert: KafkaConsumerLag
  expr: kafka_consumer_lag > 10000
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High consumer lag detected"

- alert: MessageProcessingErrors
  expr: error_rate > 5
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "High error rate in message processing"
```

## 🔧 **Configuration**

### **Producer Configuration**
```yaml
# application.yml for producer
kafka:
  producer:
    acks: all                    # Wait for all replicas
    retries: 3                   # Retry failed sends
    batch-size: 16384           # Batch size in bytes
    linger-ms: 5                # Wait time for batching
    enable-idempotence: true    # Prevent duplicates
    compression-type: snappy    # Compress messages
```

### **Consumer Configuration** 
```yaml
# application.yml for consumer
kafka:
  consumer:
    group-id: crypto-consumer-group
    auto-offset-reset: earliest  # Start from beginning
    max-poll-records: 500       # Messages per poll
    session-timeout-ms: 30000   # Session timeout
    enable-auto-commit: true    # Auto commit offsets
```

### **Topic Configuration**
```yaml
app:
  kafka:
    topics:
      default: messages
      user-events: user-events
      transaction-events: transaction-events
      notifications: notifications
      audit-logs: audit-logs
      batch-processing: batch-processing
      real-time-events: realtime-events
      error-events: error-events
      metrics: metrics
```

## 🧪 **Testing**

### **Unit Tests**
```bash
# Run all tests
./gradlew test

# Run specific module tests
./gradlew :kafka-producer-app:test
./gradlew :kafka-consumer-app:test
./gradlew :kafka-shared:test
```

### **Integration Tests**
```bash
# Start test environment
docker-compose -f docker-compose.kafka-kraft.yml up -d

# Wait for Kafka to be ready
sleep 30

# Run integration tests
./gradlew integrationTest
```

### **Load Testing**
```bash
# Use Apache Bench for load testing
ab -n 1000 -c 10 -p test-payload.json -T application/json \
  http://localhost:8080/api/v1/messages/send

# Or use custom load testing script
./scripts/load-test.sh
```

## 🚀 **Production Deployment**

### **Kubernetes Deployment**
```yaml
# Example Kubernetes deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-producer-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: kafka-producer-app
  template:
    spec:
      containers:
      - name: kafka-producer-app
        image: kafka-producer-app:1.0.0
        ports:
        - containerPort: 8080
        - containerPort: 8081
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka-cluster:9092"
        - name: SPRING_PROFILES_ACTIVE
          value: "production"
```

### **Environment Variables**
```bash
# Producer
export KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092,kafka3:9092
export KAFKA_PRODUCER_ACKS=all
export KAFKA_PRODUCER_RETRIES=10
export SPRING_PROFILES_ACTIVE=production

# Consumer  
export KAFKA_CONSUMER_GROUP_ID=crypto-consumer-prod
export KAFKA_CONSUMER_MAX_POLL_RECORDS=1000
export JAVA_OPTS="-Xmx2g -Xms1g"
```

## ⚡ **Performance Optimization**

### **Producer Optimizations**
- **Batching**: Increase `batch-size` and `linger-ms`
- **Compression**: Use `snappy` or `lz4` compression
- **Async**: Use async sending with callbacks
- **Connection Pooling**: Reuse connections

### **Consumer Optimizations**  
- **Parallel Processing**: Increase consumer threads
- **Batch Processing**: Process messages in batches
- **Memory Management**: Tune JVM heap settings
- **Offset Management**: Use manual offset commits

### **Kafka Optimizations**
- **Partitioning**: Use proper partitioning strategy
- **Replication**: Balance replication vs performance
- **Retention**: Set appropriate retention policies
- **Cleanup**: Regular log cleanup

## 🛡️ **Security Considerations**

### **Network Security**
- Use SSL/TLS for encrypted communication
- Configure SASL for authentication
- Network isolation with VPCs/firewalls
- Regular security updates

### **Access Control**
- Kafka ACLs for topic-level permissions
- Application-level authorization
- Service accounts for applications
- Audit logging for security events

### **Data Protection**
- Encrypt sensitive message data
- PII data handling compliance
- Backup and recovery procedures
- Data retention policies

## 🔍 **Troubleshooting**

### **Common Issues**

#### **Connection Issues**
```bash
# Check Kafka connectivity
kafka-broker-api-versions --bootstrap-server localhost:9092

# Check if topics exist
kafka-topics --bootstrap-server localhost:9092 --list

# Check consumer groups
kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

#### **Performance Issues**
```bash
# Check consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group crypto-consumer-group --describe

# Monitor JVM metrics
curl http://localhost:8081/actuator/metrics/jvm.memory.used
```

#### **Message Processing Issues**
```bash
# Check application logs
docker logs kafka-consumer-app

# Check error statistics
curl http://localhost:8082/api/v1/monitoring/stats
```

## 💡 **Best Practices**

1. **Message Design**
   - Use structured messages (JSON/Avro)
   - Include message versioning
   - Add correlation IDs for tracing

2. **Error Handling**
   - Implement dead letter queues
   - Use exponential backoff for retries
   - Monitor error rates and alerts

3. **Scalability**
   - Design for horizontal scaling
   - Use consumer groups effectively  
   - Monitor partition distribution

4. **Monitoring**
   - Set up comprehensive metrics
   - Create dashboards and alerts
   - Log important business events

## 🔄 **Migration Guide**

### **From test-app to Separated Apps**
1. Update imports from test packages to kafka-shared
2. Use new API endpoints with /api/v1 prefix
3. Update Docker compositions
4. Migrate configuration files
5. Update monitoring endpoints

### **From Zookeeper to KRaft**
1. Use new KRaft Docker compose
2. Update connection strings (remove Zookeeper references)
3. Verify topic configurations
4. Test consumer group behavior

## 📚 **Additional Resources**

- [Kafka KRaft Documentation](https://kafka.apache.org/documentation/#kraft)
- [Spring Kafka Reference](https://docs.spring.io/spring-kafka/docs/current/reference/html/)
- [Microservices Patterns](https://microservices.io/patterns/)
- [Kafka Best Practices](https://kafka.apache.org/documentation/#bestpractices)

---

## 🎯 **My Architecture Thoughts**

### **Why Separated Applications?**

1. **Independent Scaling**: Producer and consumer have different scaling requirements
2. **Technology Independence**: Each service can use different tech stacks if needed
3. **Deployment Flexibility**: Deploy and update services independently
4. **Resource Optimization**: Different memory/CPU requirements
5. **Team Autonomy**: Different teams can own different services

### **Why KRaft Mode?**

1. **Simplified Operations**: No Zookeeper cluster to manage
2. **Better Performance**: Reduced latency and improved throughput
3. **Easier Scaling**: Simpler cluster management
4. **Resource Efficiency**: Lower resource overhead
5. **Future-Proof**: Zookeeper mode is being deprecated

### **Production Considerations**

1. **Multi-Zone Deployment**: Deploy across availability zones
2. **Circuit Breakers**: Prevent cascade failures
3. **Rate Limiting**: Protect against traffic spikes
4. **Graceful Shutdown**: Handle shutdown signals properly
5. **Blue-Green Deployment**: Zero-downtime deployments

This architecture provides a solid foundation for production Kafka applications with excellent scalability, monitoring, and operational characteristics! 🚀