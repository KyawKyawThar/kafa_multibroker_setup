package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	broker1Address = "localhost:9092"
	broker2Address = "localhost:9093"
	broker3Address = "localhost:9094"
	topic          = "message-logs"
)

//docker exec -it 09a180b3978e /bin/sh
//# kafka-topics.sh --version
//2.8.1 (Commit:839b886f9b732b15)

func main() {
	// Setup configuration for the producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Compression = sarama.CompressionSnappy   // Optional: Use compression for performance
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush messages every 500ms
	config.Producer.Retry.Max = 5                            // Retry up to 5 times
	config.Producer.Return.Successes = true                  // Return successes on message delivery

	config.Version = sarama.V2_8_1_0

	// Enable SASL/PLAIN authentication
	//config.Net.SASL.Enable = true
	//config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	//config.Net.SASL.User = "kafka_user"
	//config.Net.SASL.Password = "kafka_password"

	// Enable TLS if using encryption (optional but recommended for SASL/PLAIN)
	//config.Net.TLS.Enable = true
	//config.Net.TLS.Config = &tls.Config{
	//	InsecureSkipVerify: true, // Set to false in production; true for local testing only
	//}
	// Connect to the Kafka broker (replace with your Kafka broker's address)
	brokerList := []string{broker1Address, broker2Address, broker3Address}

	// Create a new async Kafka producer
	producer, err := sarama.NewAsyncProducer(brokerList, config)

	if err != nil {
		log.Fatalln("Failed to start Kafka producer:", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln("Failed to close Kafka producer:", err)
		}
	}()

	signals := make(chan os.Signal, 1)

	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Send messages in a loop using goroutines
	go func() {
		for i := 0; i < 100; i++ {
			message := fmt.Sprintf("Message %d", i)
			producer.Input() <- &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(message),
			}
			fmt.Printf("Produced message: %s\n", message)
		}
	}()

	// Wait for success and errors in a separate goroutine
	go func() {
		for {
			select {
			case success := <-producer.Successes():
				fmt.Printf("Successfully produced message using multi broker: %s\n", success.Topic)
			case err := <-producer.Errors():
				fmt.Printf("Failed to send message: %v\n", err)
			}
		}
	}()

	<-signals
	fmt.Println("Producer shutting down...")

}
