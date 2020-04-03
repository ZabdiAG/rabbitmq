package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

var (
	RABBITMQ_URI  string
	QUEUE_NAME    string
	EXCHANGE_NAME string
	CONTENT_TYPE  string
	filepath      string
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
	flag.StringVar(&QUEUE_NAME, "queue", "", "Queue to push messages")
	flag.StringVar(&EXCHANGE_NAME, "exchange", "", "Exchange to push messages")
	flag.StringVar(&filepath, "filepath", "", "filepath source messages separated by line break")
	flag.Parse()
	if QUEUE_NAME == "" && EXCHANGE_NAME == "" {
		fmt.Println("Either flag --queue or --exchange must be provided")
		os.Exit(1)
	}
	if filepath == "" {
		fmt.Println("Flag filepath is mandatory")
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

	fmt.Printf("Filepath: %s\n", filepath)
	dat, err := ioutil.ReadFile(filepath)
	check(err)
	stringDatafile := string(dat)

	lines := strings.Split(stringDatafile, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		fmt.Println(line)
		encodedBody := []byte(line)
		err = channel.Publish(
			EXCHANGE_NAME, // exchange
			QUEUE_NAME,    // routing key
			false,         // mandatory
			false,         // immediate
			amqp.Publishing{
				ContentType: CONTENT_TYPE,
				Body:        encodedBody,
			},
		)
		check(err)
	}
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
