package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)

func main() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "192.168.0.169:9092",
		"group.id":          "foo",
		"auto.offset.reset": "smallest"})

	if err != nil {
		fmt.Println(err)
	}

	topics := []string{"kafka-test"}
	err = consumer.SubscribeTopics(topics, nil)

	for {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			if e.Value != nil {
				fmt.Printf("Consumed message: %s\n", string(e.Value))
			}
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}

	consumer.Close()
}
