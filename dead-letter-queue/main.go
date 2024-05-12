package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	broker        = "localhost:9092"
	mainTopic     = "test_deadletter_topic"
	retryTopic    = "test_deadletter_retries"
	dlqTopic      = "test_deadletter_dead"
	consumerGroup = "test_deadletter"
)

type Event struct {
	ID           int `json:"id"`
	FailAttempts int `json:"fail_attempts"`
}

func produce(ctx context.Context) {
	producer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  mainTopic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}

	slog.Info("producer_started")
	for {
		var count int
		time.Sleep(time.Second)
		count++

		select {
		case <-ctx.Done():
			producer.Close()
			slog.Info("producer_stopped")
			return
		default:
			r := Event{ID: count, FailAttempts: rand.Intn(4)}
			logger := slog.With(slog.Any("record", r))
			bytes, err := json.Marshal(r)
			if err != nil {
				logger.Error("failed_to_encode_record", slog.String("error", err.Error()))
				continue
			}

			if err := producer.WriteMessages(ctx, kafka.Message{Value: bytes}); err != nil {
				logger.Info("failed_to_produce_record")
				continue
			}
			logger.Info("record_produced")
		}
	}
}

func consume(ctx context.Context) {
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       mainTopic,
		StartOffset: kafka.LastOffset,
	})

	producer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  retryTopic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}

	slog.Info("consumer_started")

	for {
		select {
		case <-ctx.Done():
			consumer.Close()
			producer.Close()
			slog.Info("consumer_stopped")
			return
		default:
			msg, err := consumer.ReadMessage(ctx)
			if err != nil {
				slog.Error("failed_to_read_message", slog.String("error", err.Error()))
				continue
			}
			var r Event
			if err := json.Unmarshal(msg.Value, &r); err != nil {
				slog.Error(
					"failed_to_decode_record",
					slog.String("error", err.Error()),
					slog.Any("msg", msg),
				)
				continue
			}

			logger := slog.With(slog.Any("record", r))
			if r.FailAttempts == 0 {
				logger.Info("record_consumed")
			} else {
				logger.Info("record_consumption_failed")
				r := RetryEvent{Event: r, Attempts: 1}
				logger := slog.With(slog.Any("record", r))
				bytes, err := json.Marshal(r)
				if err != nil {
					logger.Error("failed_to_encode_retry_record", slog.String("error", err.Error()))
					continue
				}

				if err := producer.WriteMessages(ctx, kafka.Message{Value: bytes}); err != nil {
					logger.Info("failed_to_produce_retry_record")
					continue
				}
			}
		}
	}
}

// TODO: Use message headers instead
type RetryEvent struct {
	Event
	Attempts int
}

func retrier(ctx context.Context) {
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       retryTopic,
		StartOffset: kafka.LastOffset,
	})

	producer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  dlqTopic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}

	producerDLQ := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  dlqTopic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}

	slog.Info("retrier_started")

	for {
		select {
		case <-ctx.Done():
			consumer.Close()
			producer.Close()
			producerDLQ.Close()
			slog.Info("retrier_stopped")
			return
		default:
			var r RetryEvent
			msg, err := consumer.ReadMessage(ctx)
			if err != nil {
				slog.Error("failed_to_read_retry_message", slog.String("error", err.Error()))
				continue
			}

			if err := json.Unmarshal(msg.Value, &r); err != nil {
				slog.Error(
					"failed_to_decode_record",
					slog.String("error", err.Error()),
					slog.Any("msg", msg),
				)
				continue
			}
			logger := slog.With(slog.Any("record", r))
			if r.FailAttempts <= r.Attempts {
				logger.Info("record_retry_succedeed")
				continue
			}

			r.Attempts++
			bytes, err := json.Marshal(r)
			if err != nil {
				logger.Error("failed_to_encode_retry_record", slog.String("error", err.Error()))
				continue
			}

			if r.Attempts > 3 {
				logger.Info("record_reached_maximum_attempts")
				if err := producerDLQ.WriteMessages(ctx, kafka.Message{Value: bytes}); err != nil {
					logger.Info("failed_to_produce_retry_record")
					continue
				}
			} else {
				logger.Info("record_retry_failed")
				if err := producer.WriteMessages(ctx, kafka.Message{Value: bytes}); err != nil {
					logger.Info("failed_to_produce_retry_record")
					continue
				}
			}

			time.Sleep(time.Second)
		}
	}
}

func deadletter(ctx context.Context) {
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       dlqTopic,
		StartOffset: kafka.LastOffset,
	})

	slog.Info("dead_letter_queue_started")
	for {
		select {
		case <-ctx.Done():
			consumer.Close()
			slog.Info("dead_letter_queue_stopped")
			return
		default:
			msg, err := consumer.ReadMessage(ctx)
			if err != nil {
				slog.Error("failed_to_read_dlq_message", slog.String("error", err.Error()))
				continue
			}

			var r RetryEvent
			if err := json.Unmarshal(msg.Value, &r); err != nil {
				slog.Error(
					"failed_to_decode_record",
					slog.String("error", err.Error()),
					slog.Any("msg", msg),
				)
				continue
			}
			logger := slog.With(slog.Any("record", r))
			logger.Info("record_received_in_dead_letter_queue")
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Kill, os.Interrupt)

	go produce(ctx)
	go consume(ctx)
	go retrier(ctx)
	go deadletter(ctx)

	<-c
	cancel()
}
