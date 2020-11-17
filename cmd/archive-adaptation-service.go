package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	pod "github.com/filetrust/archive-adaptation-service/pkg"
	"github.com/streadway/amqp"
)

const (
	ok        = "ok"
	jsonerr   = "json_error"
	k8sclient = "k8s_client_error"
	k8sapi    = "k8s_api_error"
)

var (
	routingKey = "archive-adaptation-request"
	queueName  = "archive-adaptation-request-queue"

	procTime = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "gw_archive_adaptation_message_processing_time_millisecond",
			Help:    "Time taken to process queue message",
			Buckets: []float64{5, 10, 100, 250, 500, 1000},
		},
	)

	msgTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gw_archive_adaptation_messages_consumed_total",
			Help: "Number of messages consumed from Rabbit",
		},
		[]string{"status"},
	)

	podNamespace             = os.Getenv("POD_NAMESPACE")
	amqpURL                  = os.Getenv("AMQP_URL")
	exchange                 = os.Getenv("EXCHANGE")
	inputMount               = os.Getenv("INPUT_MOUNT")
	outputMount              = os.Getenv("OUTPUT_MOUNT")
	archiveProcessingImage   = os.Getenv("ARCHIVE_PROCESSING_IMAGE")
	archiveProcessingTimeout = os.Getenv("ARCHIVE_PROCESSING_TIMEOUT")
)

func main() {
	if podNamespace == "" || amqpURL == "" || exchange == "" || inputMount == "" || outputMount == "" || archiveProcessingImage == "" || archiveProcessingTimeout == "" {
		log.Fatalf("init failed: POD_NAMESPACE, AMQP_URL, EXCHANGE, INPUT_MOUNT, OUTPUT_MOUNT, ARCHIVE_PROCESSING_IMAGE or ARCHIVE_PROCESSING_TIMEOUT environment variables not set")
	}

	conn, err := amqp.Dial(amqpURL)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(exchange, "direct", true, false, false, false, nil)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(q.Name, routingKey, exchange, false, nil)
	failOnError(err, "Failed to bind queue")

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			requeue, err := processMessage(d)
			if err != nil {
				log.Printf("Failed to process message: %v", err)
				ch.Nack(d.DeliveryTag, false, requeue)
			}
		}
	}()

	log.Printf("[*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func processMessage(d amqp.Delivery) (bool, error) {
	defer func(start time.Time) {
		procTime.Observe(float64(time.Since(start).Milliseconds()))
	}(time.Now())

	archiveFileID := d.Headers["archive-file-id"].(string)
	input := d.Headers["source-file-location"].(string)
	output := d.Headers["rebuilt-file-location"].(string)
	replyTo := d.Headers["outcome-reply-to"].(string)

	log.Printf("Received a message for file: %s", archiveFileID)

	podArgs := pod.PodArgs{
		PodNamespace:             podNamespace,
		ArchiveFileID:            archiveFileID,
		Input:                    input,
		Output:                   output,
		InputMount:               inputMount,
		OutputMount:              outputMount,
		ReplyTo:                  replyTo,
		ArchiveProcessingImage:   archiveProcessingImage,
		ArchiveProcessingTimeout: archiveProcessingTimeout,
	}

	err := podArgs.GetClient()
	if err != nil {
		msgTotal.WithLabelValues(k8sclient).Inc()
		return true, fmt.Errorf("Failed to get client for cluster: %v", err)
	}

	err = podArgs.CreatePod()
	if err != nil {
		msgTotal.WithLabelValues(k8sapi).Inc()
		return true, fmt.Errorf("Failed to create pod: %v", err)
	}

	msgTotal.WithLabelValues(ok).Inc()
	return false, nil
}
