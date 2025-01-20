package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/streadway/amqp"
)

type Consumer struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	queueName string
	uploadDir string
}

func NewConsumer(amqpUrl, queueName, uploadDir string) (*Consumer, error) {
	conn, err := amqp.Dial(amqpUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Rebbit: %w", err)

	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	if err := os.MkdirAll(uploadDir, os.ModePerm); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to create upload Dir: %w", err)
	}

	return &Consumer{
		conn:      conn,
		channel:   ch,
		queueName: queueName,
		uploadDir: uploadDir,
	}, nil

}

func (c *Consumer) Consume() error {
	msgs, err := c.channel.Consume(
		c.queueName, //queue
		"",          //consumer
		true,        // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}
	log.Println(" [x] Waiting for messages. To exit press CTRL+C")
	for msg := range msgs {
		fileName, ok := msg.Headers["file_name"].(string)
		if !ok {
			log.Println("missing or invalid 'file_name' header")
			continue
		}

		filePath := filepath.Join(c.uploadDir, fileName)
		if err := os.WriteFile(filePath, msg.Body, 0644); err != nil {
			log.Printf("failed to write file %s: %v\n", fileName, err)
		}
		log.Printf("File saved successfully: %s\n", filePath)
	}
	return nil
}

func (c *Consumer) Close() {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}

const (
	uploadDir   = "./upload"
	rabbitMQURL = "amqp://guest:guest@localhost:5672/"
	queueName   = "file_queue"
)

func main() {
	consumer, err := NewConsumer(rabbitMQURL, queueName, uploadDir)
	if err != nil {
		log.Fatalf("error initializing consumer: %v", err)
	}
	defer consumer.Close()

	if err := consumer.Consume(); err != nil {
		log.Fatalf("error consumming messages: %v", err)
	}
}
