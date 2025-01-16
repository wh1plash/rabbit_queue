package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const archiveDir = "./archive"

var (
	fileMutex     sync.Mutex
	fileFirstSeen = make(map[string]time.Time)
)

type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       map[string]interface{}
}

func NewQueue() *QueueConfig {
	return &QueueConfig{
		Name:       "file_queue",
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,
	}
}

func declareQueue(ch *amqp.Channel, config *QueueConfig) (amqp.Queue, error) {
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

type ExchangeConfig struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       map[string]interface{}
}

func NewExchange() *ExchangeConfig {
	return &ExchangeConfig{
		Name:       "file_exchange", // Имя exchange
		Kind:       "direct",        // Тип exchange
		Durable:    true,            // Durable
		AutoDelete: false,           // Auto-deleted
		Internal:   false,           // Internal
		NoWait:     false,           // No-wait
		Args:       nil,             // Arguments
	}
}

func declareExchange(ch *amqp.Channel, config *ExchangeConfig) error {
	return ch.ExchangeDeclare(
		config.Name,
		config.Kind,
		config.Durable,
		config.AutoDelete,
		config.Internal,
		config.NoWait,
		config.Args,
	)
}

type PublishConfig struct {
	ContentType string
	Body        []byte
	Headers     amqp.Table
}

func NewPublishing(config PublishConfig) amqp.Publishing {
	return amqp.Publishing{
		ContentType: config.ContentType,
		Body:        config.Body,
		Headers:     config.Headers,
	}
}

func publishMessage(ch *amqp.Channel, config PublishConfig) error {
	message := NewPublishing(config)

	err := ch.Publish(
		"file_exchange", // exchange
		"file_route",    // routing key
		false,           // mandatory
		false,           // immediate
		message,
	)
	if err != nil {
		return err
	}

	return nil
}

type ConnectionConfig struct {
	Url string
}

type Connection struct {
	Config *ConnectionConfig
	Conn   *amqp.Connection
	Ch     *amqp.Channel
}

func NewConnectionConfig(url string) *ConnectionConfig {
	return &ConnectionConfig{
		Url: url,
	}
}

func NewConnection(config *ConnectionConfig) (*Connection, error) {
	conn, err := amqp.Dial(config.Url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &Connection{
		Config: config,
		Conn:   conn,
		Ch:     ch,
	}, nil
}

func (c *Connection) Close() {
	if c.Conn != nil {
		c.Conn.Close()
	}
	if c.Ch != nil {
		c.Ch.Close()
	}
}

func main() {
	config := NewConnectionConfig("amqp://guest:guest@localhost:5672/")
	amqpConn, err := NewConnection(config)
	if err != nil {
		log.Fatalf("Error connecting to RabbitMQ: %v", err)
	}
	defer amqpConn.Close()

	exchange := NewExchange()
	err = declareExchange(amqpConn.Ch, exchange)
	if err != nil {
		log.Fatalf("Failed to declare an exchange: %v", err)
	}
	fmt.Printf(" [x] Exchnge: %s created\n", exchange.Name)

	queue, err := declareQueue(amqpConn.Ch, NewQueue())
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}
	fmt.Printf(" [x] Queue: %s created\n", queue.Name)
	fmt.Printf(" [------>] messages in queue \"%s\": %d\n", queue.Name, queue.Messages)

	err = amqpConn.Ch.QueueBind(
		queue.Name,
		"file_route",
		"file_exchange",
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to bind queue: %v", err)
	}
	fmt.Printf(" [x] queue \"%s\" bind to \"%s\" \n", queue.Name, exchange.Name)

	stay := make(chan bool)
	// var wg sync.WaitGroup
	fileChan := make(chan string)
	go watchFiles(fileChan)
	go sendFiles(fileChan, amqpConn.Ch)
	// wg.Wait()
	<-stay
}

func watchFiles(ch chan<- string) {
	fmt.Println("Start monitoring Current folder:")

	for {
		files, err := os.ReadDir("./source")
		if err != nil {
			fmt.Println("error reading directory:", err)
			time.Sleep(time.Second * 1)
			continue
		}

		currentFiles := make(map[string]bool)

		for _, file := range files {
			if !file.IsDir() {
				filePath := filepath.Join("./source", file.Name())
				currentFiles[filePath] = true

				fileMutex.Lock()
				if _, exists := fileFirstSeen[filePath]; !exists {
					fileFirstSeen[filePath] = time.Now()
					fmt.Printf("New file detected: %s\n", filePath)
				}
				fileMutex.Unlock()

				if isFileUnchanged(filePath) {
					fmt.Printf("File %s has not been modified for more than 10 seconds. Moving...\n", filePath)
					ch <- filePath
				} else {
					fmt.Printf("File %s is not ready to be moved yet \n", filePath)
				}

			}

		}

		fileMutex.Lock()
		for filePath := range fileFirstSeen {
			if !currentFiles[filePath] {
				delete(fileFirstSeen, filePath)
				fmt.Printf("File has been removed from tracking: %s\n", filePath)
			}
		}
		fileMutex.Unlock()
		time.Sleep(time.Second)

	}

}

func isFileUnchanged(filePath string) bool {
	fileMutex.Lock()
	firstSeen, exists := fileFirstSeen[filePath]
	fileMutex.Unlock()

	if !exists {
		return false
	}

	return time.Since(firstSeen) > 4*time.Second
}

func sendFiles(ch <-chan string, Ch *amqp.Channel) {
	for fileInChan := range ch {
		fmt.Printf(" [x] Starting send file %s to queue\n", fileInChan)
		fileName := filepath.Base(fileInChan)
		fileBody, err := os.ReadFile(fileInChan)
		if err != nil {
			log.Fatalf("Failed to open file: %v", err)
		}

		messageConfig := PublishConfig{
			ContentType: "text/plain",
			Body:        fileBody,
			Headers: amqp.Table{
				"file_name": fileName,
			},
		}

		err = publishMessage(Ch, messageConfig)
		if err != nil {
			log.Fatalf("Failed to publish message: %v", err)
		}
		fmt.Printf(" [x] File [%s] has been succesfuly send to RabbitMQ\n", fileName)

		moveFileToArchive(fileInChan)
	}
}

func moveFileToArchive(f string) {
	currentDate := time.Now().Format("2006-01-02")
	destDir := filepath.Join(archiveDir, currentDate)

	if err := os.MkdirAll(destDir, os.ModePerm); err != nil {
		log.Fatal(err)
	}

	archivePath := filepath.Join(destDir, filepath.Base(f))

	srcFile, err := os.Open(f)
	if err != nil {
		log.Fatal(err)
	}

	destFile, err := os.Create(archivePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}

	if _, err := io.Copy(destFile, srcFile); err != nil {
		log.Fatal(err)
	}
	srcFile.Close()
	destFile.Close()

	err = os.Remove(f)
	if err != nil {
		log.Fatal(err)
	}

}
