#!/bin/bash

echo "Starting Kafka Producer Application..."

# Build the application
cd ..
./gradlew :test-app:build -x test

# Run the producer application
cd test-app
java -jar build/libs/test-app-1.0.0.jar \
  --spring.profiles.active=producer \
  --spring.main.class=com.crp.test.producer.KafkaProducerApplication

echo "Producer application started on port 8080"