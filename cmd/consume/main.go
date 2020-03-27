package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
)

var (
	RABBITMQ_URI string
	QUEUE_NAME   string
	format       string
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func InitEnvs() {
	checkENV := func(env string, name string) {
		if env == "" {
			fmt.Println(errors.New(fmt.Sprintf("ENV %s is undefined", name)))
			os.Exit(1)
		}
	}
	RABBITMQ_URI = os.Getenv("RABBITMQURI")
	checkENV(RABBITMQ_URI, "RABBITMQURI")
}

func GetARGVFlags() {
	flag.StringVar(&QUEUE_NAME, "queue", "", "Queue to pull messages")
	flag.StringVar(&format, "format", "", "Format message")
	flag.Parse()
	if QUEUE_NAME == "" {
		fmt.Println("queue parameter is undefined")
		os.Exit(1)
	}
}

func main() {
	InitEnvs()
	GetARGVFlags()
	rabbitConnection, err := amqp.Dial(RABBITMQ_URI)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer rabbitConnection.Close()
	channel, err := rabbitConnection.Channel()
	failOnError(err, "Failed to open a channel")
	defer channel.Close()
	err = channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")
	consumer, err := channel.Consume(
		QUEUE_NAME, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	failOnError(err, "Failed to register a consumer")
	forever := make(chan bool)
	go func() {
		for message := range consumer {
			// var parsedMessage parser.Message
			// json.Unmarshal(message.Body, &parsedMessage)
			fmt.Println(string(message.Body))
			err = message.Ack(false)
			failOnError(err, "Failed to ack message")
		}
	}()
	<-forever
}
