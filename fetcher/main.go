package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/streadway/amqp"
)

type FileTask struct {
	FilePath        string `json:"filePath"`
	DestinationPath string `json:"destination"`
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()

	q, err := ch.QueueDeclare(
		"file_tasks", // name
		true,         // durable
		true,         // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	forever := make(chan bool)

	go func() {
		for i := range msgs {
			var task FileTask
			if err := json.Unmarshal(i.Body, &task); err != nil {
				log.Printf("Failed to unmarshal a message: %v", err)
				continue
			}
			log.Printf("Received a file task: %v", task)
			processFile(task)
		}
	}()

	fmt.Println(" [x] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func processFile(task FileTask) {
	fmt.Println("Processing file:", task.FilePath)

	targetPath := filepath.Join(task.DestinationPath, filepath.Base(task.FilePath))

	if err := moveFile(task.FilePath, targetPath); err != nil {
		log.Printf("Failed to move file: %v", err)
	}

	fmt.Printf("Move file %s to %s\n", task.FilePath, task.DestinationPath)
}

func moveFile(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), os.ModePerm); err != nil {
		return err
	}

	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	return os.Remove(src)

}
