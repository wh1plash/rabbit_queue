package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/streadway/amqp"
)

const uploadDir = "./upload"

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
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}

	q, err := ch.QueueDeclare(
		"file_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	err = ch.QueueBind(
		q.Name,
		"file_route",
		"file_exchange",
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to bind queue: %v", err)
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

	if err := os.MkdirAll(uploadDir, os.ModePerm); err != nil {
		log.Fatalf("Error creating %s directory\n", uploadDir)
	}

	quitch := make(chan bool)

	go func() {
		for i := range msgs {
			fileName := i.Headers["file_name"].(string)
			fmt.Println("Receive file:", fileName)

			err := os.WriteFile(filepath.Join(uploadDir, fileName), i.Body, 0644)
			if err != nil {
				log.Fatalf("Failed to write file: %v", err)
			}
			fmt.Println("File saved successfully", fileName)
		}
	}()

	fmt.Println(" [x] Waiting for messages. To exit press CTRL+C")
	<-quitch
}

// func processFile(task FileTask) {
// 	fmt.Println("Processing file:", task.FilePath)

// 	targetPath := filepath.Join(task.DestinationPath, filepath.Base(task.FilePath))

// 	if err := moveFile(task.FilePath, targetPath); err != nil {
// 		log.Printf("Failed to move file: %v", err)
// 	}

// 	fmt.Printf("Move file %s to %s\n", task.FilePath, task.DestinationPath)
// }

// func moveFile(src, dst string) error {
// 	if err := os.MkdirAll(filepath.Dir(dst), os.ModePerm); err != nil {
// 		return err
// 	}

// 	srcFile, err := os.Open(src)
// 	if err != nil {
// 		return err
// 	}
// 	defer srcFile.Close()

// 	dstFile, err := os.Create(dst)
// 	if err != nil {
// 		return err
// 	}

// 	if _, err := io.Copy(dstFile, srcFile); err != nil {
// 		return err
// 	}

// 	return os.Remove(src)

// }
