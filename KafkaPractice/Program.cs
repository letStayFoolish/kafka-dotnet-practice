using Confluent.Kafka;

namespace KafkaPractice;

/// <summary>
/// Simple Kafka learning application demonstrating Producer and Consumer
/// Prerequisites: Kafka broker running on localhost:9092
/// </summary>
class Program
{
    private const string BootstrapServers = "localhost:9092";
    private const string TopicName = "learning-topic";

    static void Main(string[] args)
    {
        Console.WriteLine("=== Kafka Learning Application ===\n");
        Console.WriteLine("1. Run Producer");
        Console.WriteLine("2. Run Consumer");
        Console.Write("\nYour choice: ");

        var choice = Console.ReadLine();

        switch (choice)
        {
            case "1":
                RunProducer();
                break;
            case "2":
                RunConsumer();
                break;
            default:
                Console.WriteLine("Invalid choice!");
                break;
        }
    }

    /// <summary>
    /// Producer: Sends messages to Kafka topic
    /// Demonstrates: message keys, partitioning, delivery reports
    /// </summary>
    static void RunProducer()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();

        Console.WriteLine($"\n✓ Producer connected to {BootstrapServers}");
        Console.WriteLine($"✓ Sending messages to topic: {TopicName}");
        Console.WriteLine("✓ Press Ctrl+C to stop\n");

        var messageCount = 0;

        try
        {
            while (true)
            {
                messageCount++;

                // Message key determines partition (same key = same partition)
                var key = $"user-{messageCount % 3}";
                var value = $"Message #{messageCount} at {DateTime.UtcNow:HH:mm:ss}";

                // Send message and get delivery report
                var result = producer.ProduceAsync(TopicName,
                    new Message<string, string> { Key = key, Value = value }).Result;

                Console.WriteLine(
                    $"✓ Sent: Key='{key}' | Value='{value}' | " +
                    $"Partition={result.Partition.Value} | Offset={result.Offset.Value}");

                Thread.Sleep(2000);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"✗ Error: {ex.Message}");
        }
        finally
        {
            producer.Flush(TimeSpan.FromSeconds(10));
        }
    }

    /// <summary>
    /// Consumer: Receives messages from Kafka topic
    /// Demonstrates: consumer groups, partitions, offsets
    /// </summary>
    static void RunConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = "learning-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(TopicName);

        Console.WriteLine($"\n✓ Consumer connected to {BootstrapServers}");
        Console.WriteLine($"✓ Subscribed to topic: {TopicName}");
        Console.WriteLine($"✓ Consumer Group: {config.GroupId}");
        Console.WriteLine("✓ Press Ctrl+C to stop\n");

        try
        {
            while (true)
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(1));

                if (result != null)
                {
                    Console.WriteLine(
                        $"✓ Received: Key='{result.Message.Key}' | " +
                        $"Value='{result.Message.Value}' | " +
                        $"Partition={result.Partition.Value} | " +
                        $"Offset={result.Offset.Value}");
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"✗ Error: {ex.Message}");
        }
        finally
        {
            consumer.Close();
        }
    }
}