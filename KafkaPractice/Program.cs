using Confluent.Kafka;

namespace KafkaPractice;

/// <summary>
/// Simple Kafka learning application demonstrating Producer, Consumer, Topics, and Partitions
/// Prerequisites: Kafka broker running on localhost:9092
/// </summary>
class Program
{
    private const string BootstrapServers = "localhost:9092";
    private const string TopicName = "learning-topic";
    
    static async Task Main(string[] args)
    {
        Console.WriteLine("=== Kafka Learning Application ===\n");
        Console.WriteLine("Choose an option:");
        Console.WriteLine("1. Run Producer (send messages)");
        Console.WriteLine("2. Run Consumer (receive messages)");
        Console.WriteLine("3. Run Both (producer + consumer in parallel)");
        Console.Write("\nYour choice: ");
        
        var choice = Console.ReadLine();
        
        switch (choice)
        {
            case "1":
                await RunProducer();
                break;
            case "2":
                await RunConsumer();
                break;
            case "3":
                await RunBoth();
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
    static async Task RunProducer()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            ClientId = "learning-producer",
            // Acks.All ensures message is written to all in-sync replicas (most reliable)
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
                
                // Message key determines which partition the message goes to
                // Same key always goes to the same partition (ordering guarantee)
                var key = $"user-{messageCount % 3}"; // 3 different keys for distribution
                var value = $"Message #{messageCount} at {DateTime.Now:HH:mm:ss}";
                
                // Asynchronous send with delivery report
                var deliveryResult = await producer.ProduceAsync(
                    TopicName,
                    new Message<string, string>
                    {
                        Key = key,
                        Value = value
                    });
                
                Console.WriteLine(
                    $"✓ Sent: Key='{key}' | Value='{value}' | " +
                    $"Partition={deliveryResult.Partition.Value} | " +
                    $"Offset={deliveryResult.Offset.Value}");
                
                await Task.Delay(2000); // Send every 2 seconds
            }
        }
        catch (ProduceException<string, string> ex)
        {
            Console.WriteLine($"✗ Delivery failed: {ex.Error.Reason}");
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("\n✓ Producer stopped.");
        }
        finally
        {
            // Flush to ensure all messages are sent
            producer.Flush(TimeSpan.FromSeconds(10));
        }
    }
    
    /// <summary>
    /// Consumer: Receives messages from Kafka topic
    /// Demonstrates: consumer groups, partitions, offsets, manual commit
    /// </summary>
    static async Task RunConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = "learning-consumer-group", // Consumers in same group share partitions
            ClientId = "learning-consumer",
            // AutoOffsetReset determines where to start if no offset exists
            AutoOffsetReset = AutoOffsetReset.Earliest, // Start from beginning
            EnableAutoCommit = false // Manual commit for better control
        };
        
        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        
        // Subscribe to topic (can subscribe to multiple topics)
        consumer.Subscribe(TopicName);
        
        Console.WriteLine($"\n✓ Consumer connected to {BootstrapServers}");
        Console.WriteLine($"✓ Subscribed to topic: {TopicName}");
        Console.WriteLine($"✓ Consumer Group: {config.GroupId}");
        Console.WriteLine("✓ Press Ctrl+C to stop\n");
        
        try
        {
            while (true)
            {
                // Poll for messages (blocking call with timeout)
                var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(1000));
                
                if (consumeResult != null)
                {
                    Console.WriteLine(
                        $"✓ Received: Key='{consumeResult.Message.Key}' | " +
                        $"Value='{consumeResult.Message.Value}' | " +
                        $"Partition={consumeResult.Partition.Value} | " +
                        $"Offset={consumeResult.Offset.Value}");
                    
                    // Manual commit after processing
                    consumer.Commit(consumeResult);
                }
                
                await Task.Delay(100); // Small delay for responsiveness
            }
        }
        catch (ConsumeException ex)
        {
            Console.WriteLine($"✗ Consume error: {ex.Error.Reason}");
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("\n✓ Consumer stopped.");
        }
        finally
        {
            consumer.Close();
        }
    }
    
    /// <summary>
    /// Runs both Producer and Consumer in parallel for testing
    /// </summary>
    static async Task RunBoth()
    {
        var cts = new CancellationTokenSource();
        
        Console.WriteLine("\n✓ Starting Producer and Consumer...\n");
        Console.WriteLine("Press any key to stop both...\n");
        
        // Run producer and consumer concurrently
        var producerTask = Task.Run(async () =>
        {
            try
            {
                await RunProducer();
            }
            catch { }
        }, cts.Token);
        
        var consumerTask = Task.Run(async () =>
        {
            try
            {
                // Small delay to let producer start first
                await Task.Delay(1000);
                await RunConsumer();
            }
            catch { }
        }, cts.Token);
        
        Console.ReadKey();
        cts.Cancel();
        
        await Task.WhenAll(producerTask, consumerTask);
        Console.WriteLine("\n✓ Both Producer and Consumer stopped.");
    }
}