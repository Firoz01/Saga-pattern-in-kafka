package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "192.168.0.169:9092", // Specify the correct Kafka broker address
		"client.id":         getHostname(),
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	defer p.Close() // Make sure to close the producer when done

	deliveryChan := make(chan kafka.Event)

	go func() {
		for e := range deliveryChan {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			default:
				fmt.Printf("Ignored event: %v\n", ev)
			}
		}
	}()

	topic := "kafka-test"
	value := "kafka hello"

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(value),
	}, deliveryChan)

	if err != nil {
		fmt.Printf("Failed to produce message: %v\n", err)
		os.Exit(1)
	}

	// Wait for the delivery report to be received
	<-deliveryChan
}

func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("Error getting hostname: %s\n", err)
		return "default-client-id"
	}
	fmt.Println(hostname)
	return hostname
}
