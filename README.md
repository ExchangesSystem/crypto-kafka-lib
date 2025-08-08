# Crypto Kafka Library

A Kafka producer and consumer configuration library for Spring Boot applications.

## Features

- Pre-configured Kafka producer and consumer
- Spring Boot auto-configuration
- Configurable serialization
- Message sending utilities

## Usage

### Gradle

```gradle
dependencies {
    implementation 'com.github.YourUsername:crypto-kafka-lib:1.0.0'
}
```

### Maven

```xml
<dependency>
    <groupId>com.github.YourUsername</groupId>
    <artifactId>crypto-kafka-lib</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Configuration

Add to your `application.properties`:

```properties
kafka.bootstrap-servers=localhost:9092
kafka.consumer.group-id=your-group-id
```

## Components

- `KafkaMessageProducer` - Send messages to Kafka topics
- `KafkaProducerConfig` - Producer configuration
- `KafkaConsumerConfig` - Consumer configuration