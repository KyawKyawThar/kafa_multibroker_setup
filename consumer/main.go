package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const (
	broker1Address = "localhost:9092"
	broker2Address = "localhost:9093"
	broker3Address = "localhost:9094"
	topic          = "message-logs"
	partition      = 0
)

func main() {

	// Enable SASL/PLAIN authentication
	//config.Net.SASL.Enable = true
	//config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	//config.Net.SASL.User = "your-username"     // Replace with your Kafka username
	//config.Net.SASL.Password = "your-password" // Replace with your Kafka password
	//
	//// Enable TLS (if you're using TLS/SSL encryption)
	//config.Net.TLS.Enable = true
	//config.Net.TLS.Config.InsecureSkipVerify = true

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V2_8_1_0
	brokerList := []string{broker1Address, broker2Address, broker3Address}

	// Create a new consumer
	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Kafka consumer:", err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln("Failed to close Kafka consumer:", err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)

	if err != nil {
		log.Fatalln("Failed to consume partition:", err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln("Failed to close partition consumer:", err)
		}
	}()

	// Handle OS signals to safely exit the consumer
	signals := make(chan os.Signal, 1)

	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Starting to consume messages...")

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("Consumed message: %s, offset: %d\n", string(msg.Value), msg.Offset)
		case err := <-partitionConsumer.Errors():
			fmt.Println("Failed to consume message:", err)
		case <-signals:
			fmt.Println("Interrupt signal received, shutting down...")
			return
		}
	}
}
