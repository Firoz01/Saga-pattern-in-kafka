package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"os"
)

var producer *kafka.Producer
var consumer *kafka.Consumer

func main() {
	// Initialize the Kafka producer
	initKafkaProducer()
	initKafkaConsumer()
	// Create an Echo instance
	e := echo.New()

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// Routes
	//e.POST("/publish", publishMessage)

	// Start the server
	e.Start(":8081")
}

func initKafkaProducer() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "192.168.0.169:9092", // Specify the correct Kafka broker address
		"client.id":         getHostname(),
	})

	if err != nil {
		fmt.Printf("Failed to create Kafka producer: %s\n", err)
		os.Exit(1)
	}

	producer = p

	go func() {
		for e := range producer.Events() {
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
}

func initKafkaConsumer() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "192.168.0.169:9092",
		"group.id":           "my-group", // Specify your consumer group ID
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})

	if err != nil {
		fmt.Printf("Failed to create Kafka consumer: %s\n", err)
		os.Exit(1)
	}

	consumer = c

	err = consumer.SubscribeTopics([]string{"kafka-one"}, nil)
	if err != nil {
		fmt.Printf("Error subscribing to topic: %v\n", err)
		os.Exit(1)
	}

	go func() {
		for {
			msg, err := consumer.ReadMessage(-1)
			if err == nil {
				fmt.Printf("Consumed message from Kafka: %s\n", string(msg.Value))
				// Process the returned message here
				publishMessage(string(msg.Value), "kafka-two")
			} else {
				fmt.Printf("Error reading message from Kafka: %v\n", err)
			}
		}
	}()
}

func publishMessage(message string, topic string) error {
	//topic := "kafka-two"

	modifyMessage := message + " modified"

	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(modifyMessage),
	}, nil)

	if err != nil {
		fmt.Println("failed to produced message from server two")
	}
	return err
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
