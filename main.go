package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

type EmailRequest struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Subject string `json:"subject"`
	Text    string `json:"text"`
	HTML    string `json:"html"`
}

var redisSvc *RedisService

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Printf("Warning: could not load .env: %v\n", err)
	}
	redisSvc = NewRedisService(
		os.Getenv("REDIS_ADDR"),
		"go-email",
		10000,
	)
}

func main() {
	broker := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if broker == "" {
		broker = "kafka:29092"
	}

	topic := os.Getenv("KAFKA_EMAIL_TOPIC")
	if topic == "" {
		topic = "segurapass-email"
	}

	groupID := os.Getenv("KAFKA_CONSUMER_GROUP")
	if groupID == "" {
		groupID = "segurapass-go-email-consumer"
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	log.Printf(
		"Email worker started. Kafka=%s topic=%s group=%s",
		broker,
		topic,
		groupID,
	)

	for {
		if ctx.Err() != nil {
			break
		}

		reader := createKafkaReader(broker, topic, groupID)

		log.Println("Kafka consumer connecting...")

		err := consumeMessages(ctx, reader)

		reader.Close()

		if ctx.Err() != nil {
			break
		}

		log.Printf(
			"Kafka consumer stopped: %v",
			err,
		)

		log.Println("Retrying Kafka consumer connection in 5 seconds...")

		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}

	log.Println("Email worker shutting down")
}

func createKafkaReader(broker, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   topic,
		GroupID: groupID,
	})
}

func consumeMessages(ctx context.Context, reader *kafka.Reader) error {
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			return fmt.Errorf("Kafka receive error: %w", err)
		}

		var req EmailRequest

		if err := json.Unmarshal(message.Value, &req); err != nil {
			log.Printf(
				"Invalid email message at offset %d: %v",
				message.Offset,
				err,
			)

			// Commit malformed messages so they don't block
			// the consumer forever.
			if err := reader.CommitMessages(ctx, message); err != nil {
				return fmt.Errorf(
					"failed to commit invalid message at offset %d: %w",
					message.Offset,
					err,
				)
			}

			continue
		}

		log.Printf(
			"Processing email for %s (offset %d)",
			req.To,
			message.Offset,
		)

		if err := SendEmail(ctx, req, redisSvc); err != nil {
			log.Printf(
				"Failed to send email at offset %d: %v",
				message.Offset,
				err,
			)

			// Do not commit the message.
			// Kafka will make it available again.
			continue
		}

		if err := reader.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf(
				"failed to commit Kafka message at offset %d: %w",
				message.Offset,
				err,
			)
		}

		log.Printf(
			"Email successfully processed at offset %d",
			message.Offset,
		)
	}
}
