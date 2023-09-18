package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"net/http"
	"os"
)

var producer *kafka.Producer
var consumer *kafka.Consumer

func main() {
	// Create an Echo instance
	e := echo.New()

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// Routes
	e.POST("/publish/transaction", publishMessageInTransaction)

	// Initialize Kafka producer and transactions
	initKafkaProducerAndTransactions()
	initKafkaConsumer()
	// Start the server
	e.Start(":8080")
}

func initKafkaProducerAndTransactions() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  "192.168.0.169:9092",
		"client.id":          getHostname(),
		"transactional.id":   "my-transactional-id",
		"acks":               "all",
		"enable.idempotence": "true",
	})

	if err != nil {
		fmt.Printf("Failed to create Kafka producer: %s\n", err)
		os.Exit(1)
	}

	// Initialize Kafka transactions
	ctx := context.Background()
	err = p.InitTransactions(ctx)
	if err != nil {
		fmt.Printf("Failed to initialize transactions: %s\n", err)
		os.Exit(1)
	}

	producer = p
}

func publishMessageInTransaction(c echo.Context) error {
	topic := "kafka-one"
	value := c.FormValue("message")

	//ctx := c.Request().Context()

	// Begin a new transaction
	err := producer.BeginTransaction()
	if err != nil {
		return c.String(http.StatusInternalServerError, fmt.Sprintf("Failed to begin transaction: %v\n", err))
	}

	// Produce the message within the transaction
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic,             : kafka.PartitionAny},
		Value:          []byte(value),
	}, nil)

	if err != nil {
		// Abort the transaction if producing the message fails
		producer.AbortTransaction(nil)
		return c.String(http.StatusInternalServerError, fmt.Sprintf("Failed to produce message within transaction: %v\n", err))
	}

	// Commit the transaction
	err = producer.CommitTransaction(nil)
	if err != nil {
		return c.String(http.StatusInternalServerError, fmt.Sprintf("Failed to commit transaction: %v\n", err))
	}

	return c.String(http.StatusOK, "Message published to Kafka within a transaction")
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

	err = consumer.SubscribeTopics([]string{"kafka-two"}, nil)
	if err != nil {
		fmt.Printf("Error subscribing to topic: %v\n", err)
		os.Exit(1)
	}

	go func() {
		for {
			msg, err := consumer.ReadMessage(-1)
			if err == nil {
				fmt.Printf("Consumed message from Kafka: %s\n", string(msg.Value))
			} else {
				fmt.Printf("Error reading message from Kafka: %v\n", err)
			}
		}
	}()
}

func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("Error getting hostname: %s\n", err)
		return "default-client-id"
	}
	return hostname
}
