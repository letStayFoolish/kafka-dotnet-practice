# Kafka .NET Practice

Simple, beginner-friendly Kafka learning application focused on core concepts.

## 🎯 What's Included

**Producer** - Sends messages every 2 seconds
- Message keys (user-0, user-1, user-2) for partition distribution
- Timestamps in messages
- Delivery reports showing partition & offset

**Consumer** - Receives messages
- Consumer group for load balancing
- Displays partition & offset info

## 📋 Prerequisites

Kafka running locally. Quick start with Docker:
```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:latest
```

## 🚀 How to Run
```bash
dotnet run
```

Choose: 1 (Producer) or 2 (Consumer)

Run in two separate terminals to see producer and consumer working together.

## 🔑 Key Kafka Concepts Demonstrated
- **Topics**: Logical channel for messages (`learning-topic`)
- **Partitions**: Messages distributed by key hash (same key = same partition)
- **Producer**: Sends with `Acks.All` for reliability
- **Consumer**: Uses consumer group, auto-commit, earliest offset
- **Keys**: Enable ordering guarantee within partition
- **Offsets**: Track message position per partition