#!/bin/bash

echo "Starting Kafka Consumer Application..."

# Build the application
cd ..
./gradlew :test-app:build -x test

# Run the consumer application
cd test-app
java -jar build/libs/test-app-1.0.0.jar \
  --spring.profiles.active=consumer \
  --spring.main.class=com.crp.test.consumer.KafkaConsumerApplication

echo "Consumer application started on port 8081"