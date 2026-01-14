## 🎯 What's Included

### Producer - Sends dynamic messages every 2 seconds with:
- Message keys (user-0, user-1, user-2) for partition distribution
- Timestamps in messages
- Delivery reports showing partition & offset

### Consumer - Receives messages with:
- Consumer group for load balancing
- Manual offset commit for control
- Displays partition & offset info

**Both Mode** - Run producer + consumer together for immediate testing

## 📋 Prerequisites

You need Kafka running locally. Quick start with Docker:
```docker
docker run -d --name kafka -p 9092:9092 apache/kafka:latest
```
## 🚀 How to Run
```shell
# Restore packages
dotnet restore

# Run the app
dotnet run
```

Then choose: 1 (Producer), 2 (Consumer), or 3 (Both)

## 🔑 Key Kafka Concepts Demonstrated
- **Topics**: `learning-topic` - logical channel for messages
- **Partitions**: Messages distributed by key hash (same key → same partition)
- **Producer**: Sends with `Acks.All` for reliability
- **Consumer**: Uses consumer group, manual commits, earliest offset
- **Keys**: Enable ordering guarantee within partition
- **Offsets**: Track message position per partition

Try running option 3 first to see both working together!