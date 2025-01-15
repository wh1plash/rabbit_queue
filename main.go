package main

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       map[string]interface{}
}

func declareQueue(ch *amqp.Channel, config QueueConfig) (amqp.Queue, error) {
	args := amqp.Table{}
	for k, v := range config.Args {
		args[k] = v
	}

	return ch.QueueDeclare(
		config.Name,
		config.Durable,
		config.AutoDelete,
		config.Exclusive,
		config.NoWait,
		args,
	)
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	// err = ch.ExchangeDeclare(
	// 	"dlx_exchange", // Имя DLX
	// 	"direct",       // Тип exchange
	// 	true,           // Durable
	// 	false,          // Auto-deleted
	// 	false,          // Internal
	// 	false,          // No-wait
	// 	nil,            // Arguments
	// )
	// if err != nil {
	// 	log.Fatalf("Failed to declare DLX: %v", err)
	// }
	// fmt.Println("DLX exchange created")

	queueConfig := QueueConfig{
		Name:       "file_tasks",
		Durable:    true,
		AutoDelete: true,
		Exclusive:  false,
		NoWait:     false,
		Args: nil,
	}

	queue, err := declareQueue(ch, queueConfig)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	fmt.Printf("Primary queue created: %s\n", queue.Name)

	for i := 1; i <= 6; i++ {

		filePath := "/home/volodymyr/Oleh/go/project/rabbit/source"
		destination := "/home/volodymyr/Oleh/go/project/rabbit/destination"

		body := fmt.Sprintf("{\"filePath\": \"%s/%d.txt\", \"destination\": \"%s\"}", filePath, i, destination)
		//fmt.Println("Generated message: ", body)

		err = ch.Publish(
			"",         // exchange
			queue.Name, // routing key
			false,      // mandatory
			false,      // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        []byte(body),
			},
		)
		if err != nil {
			log.Fatalf("Failed to publis a message: %v", err)
		}
		fmt.Println(" [x] Send", body)
	}
}
