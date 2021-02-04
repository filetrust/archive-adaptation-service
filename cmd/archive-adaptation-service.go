package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	pod "github.com/filetrust/archive-adaptation-service/pkg"
	"github.com/filetrust/archive-adaptation-service/pkg/comms"
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

	podNamespace                          = os.Getenv("POD_NAMESPACE")
	exchange                              = os.Getenv("EXCHANGE")
	metricsPort                           = os.Getenv("METRICS_PORT")
	pushgatewayEndpoint                   = os.Getenv("PUSHGATEWAY_ENDPOINT")
	inputMount                            = os.Getenv("INPUT_MOUNT")
	outputMount                           = os.Getenv("OUTPUT_MOUNT")
	archiveProcessingImage                = os.Getenv("ARCHIVE_PROCESSING_IMAGE")
	archiveProcessingTimeout              = os.Getenv("ARCHIVE_PROCESSING_TIMEOUT")
	archiveAdaptationRequestQueueHostname = os.Getenv("ARCHIVE_ADAPTATION_REQUEST_QUEUE_HOSTNAME")
	archiveAdaptationRequestQueuePort     = os.Getenv("ARCHIVE_ADAPTATION_REQUEST_QUEUE_PORT")
	adaptationRequestQueueHostname        = os.Getenv("ADAPTATION_REQUEST_QUEUE_HOSTNAME")
	adaptationRequestQueuePort            = os.Getenv("ADAPTATION_REQUEST_QUEUE_PORT")
	messageBrokerUser                     = os.Getenv("MESSAGE_BROKER_USER")
	messageBrokerPassword                 = os.Getenv("MESSAGE_BROKER_PASSWORD")
)

func main() {
	if podNamespace == "" || exchange == "" || metricsPort == "" || pushgatewayEndpoint == "" || inputMount == "" || outputMount == "" || archiveProcessingImage == "" || archiveProcessingTimeout == "" {
		log.Fatalf("init failed: POD_NAMESPACE, EXCHANGE, METRICS_PORT, PUSHGATEWAY_ENDPOINT, INPUT_MOUNT, OUTPUT_MOUNT, ARCHIVE_PROCESSING_IMAGE or ARCHIVE_PROCESSING_TIMEOUT environment variables not set")
	}

	if archiveAdaptationRequestQueueHostname == "" || adaptationRequestQueueHostname == "" {
		log.Fatalf("init failed: ARCHIVE_ADAPTATION_REQUEST_QUEUE_HOSTNAME or ADAPTATION_REQUEST_QUEUE_HOSTNAME environment variables not set")
	}

	if archiveAdaptationRequestQueuePort == "" || adaptationRequestQueuePort == "" {
		log.Fatalf("init failed: ARCHIVE_ADAPTATION_REQUEST_QUEUE_PORT or ADAPTATION_REQUEST_QUEUE_PORT environment variables not set")
	}

	if messageBrokerUser == "" {
		messageBrokerUser = "guest"
	}

	if messageBrokerPassword == "" {
		messageBrokerPassword = "guest"
	}

	amqpURL := url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(messageBrokerUser, messageBrokerPassword),
		Host:   fmt.Sprintf("%s:%s", adaptationRequestQueueHostname, adaptationRequestQueuePort),
		Path:   "/",
	}
	forever := make(chan bool)

	fmt.Println("Connecting to ", amqpURL.Host)

	conn := comms.NewConnection("archive-adaptation-consumer", exchange, routingKey, []string{queueName}, amqpURL)
	if err := conn.Connect(); err != nil {
		failOnError(err, "Failed to connect to RabbitMQ")
	}

	if err := conn.BindQueue(); err != nil {
		failOnError(err, "Failed to bind queue")
	}

	deliveries, err := conn.Consume()
	if err != nil {
		failOnError(err, "Failed to register a consumer")
	}

	for q, d := range deliveries {
		go conn.HandleConsumedDeliveries(q, d, messageHandler)
	}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(fmt.Sprintf(":%v", metricsPort), nil)
	}()

	log.Printf("[*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func messageHandler(c comms.Connection, q string, deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		requeue, err := processMessage(d)
		if err != nil {
			log.Printf("Failed to process message: %v", err)
			d.Nack(false, requeue)
		}
	}
}

func processMessage(d amqp.Delivery) (bool, error) {
	defer func(start time.Time) {
		procTime.Observe(float64(time.Since(start).Milliseconds()))
	}(time.Now())

	if d.Headers["archive-file-id"] == nil ||
		d.Headers["archive-file-type"] == nil ||
		d.Headers["source-file-location"] == nil ||
		d.Headers["rebuilt-file-location"] == nil ||
		d.Headers["outcome-reply-to"] == nil {
		return false, fmt.Errorf("Header value is nil")
	}

	archiveFileID := d.Headers["archive-file-id"].(string)
	archiveFileType := d.Headers["archive-file-type"].(string)
	input := d.Headers["source-file-location"].(string)
	output := d.Headers["rebuilt-file-location"].(string)
	replyTo := d.Headers["outcome-reply-to"].(string)

	log.Printf("Received a message for file: %s", archiveFileID)

	podArgs := pod.PodArgs{
		PodNamespace:                   podNamespace,
		ArchiveFileID:                  archiveFileID,
		ArchiveFileType:                archiveFileType,
		Input:                          input,
		Output:                         output,
		InputMount:                     inputMount,
		OutputMount:                    outputMount,
		ReplyTo:                        replyTo,
		ArchiveProcessingImage:         archiveProcessingImage,
		ArchiveProcessingTimeout:       archiveProcessingTimeout,
		AdaptationRequestQueueHostname: adaptationRequestQueueHostname,
		AdaptationRequestQueuePort:     adaptationRequestQueuePort,
		MessageBrokerUser:              messageBrokerUser,
		MessageBrokerPassword:          messageBrokerPassword,
		PushGatewayEndpoint:            pushgatewayEndpoint,
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
