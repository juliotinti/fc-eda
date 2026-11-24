package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-chi/chi/v5"
	_ "github.com/go-sql-driver/mysql"
)

type balanceUpdated struct {
	Name    string
	Payload struct {
		AccountFrom string       `json:"account_id_from"`
		AccountTo   string       `json:"account_id_to"`
		BalanceFrom *json.Number `json:"balance_account_id_from"`
		BalanceTo   *json.Number `json:"balance_account_id_to"`
	}
}

func main() {
	db, err := sql.Open("mysql", os.Getenv("BALANCES_DSN"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	consumer, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers":  os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		"group.id":           "balances-service",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	if err := consumer.Subscribe("balances", nil); err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := consume(db, consumer); err != nil {
			log.Fatal(err)
		}
	}()

	router := chi.NewRouter()
	router.Get("/balances/{account_id}", func(w http.ResponseWriter, r *http.Request) {
		accountID := chi.URLParam(r, "account_id")
		var balance json.Number
		err := db.QueryRowContext(r.Context(),
			"SELECT balance FROM balances WHERE account_id = ?", accountID).Scan(&balance)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			http.Error(w, "account not found", http.StatusNotFound)
		case err != nil:
			log.Printf("query balance: %v", err)
			http.Error(w, "could not query balance", http.StatusInternalServerError)
		default:
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(struct {
				AccountID string      `json:"account_id"`
				Balance   json.Number `json:"balance"`
			}{accountID, balance}); err != nil {
				log.Printf("write balance response: %v", err)
			}
		}
	})

	server := &http.Server{Addr: ":3003", Handler: router, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}

func consume(db *sql.DB, consumer *ckafka.Consumer) error {
	for {
		switch event := consumer.Poll(1000).(type) {
		case *ckafka.Message:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := applyBalanceUpdate(ctx, db, event)
			cancel()
			if err != nil {
				return fmt.Errorf("apply event %s: %w", event.TopicPartition, err)
			}
			if _, err := consumer.CommitMessage(event); err != nil {
				return fmt.Errorf("commit Kafka offset: %w", err)
			}
		case ckafka.Error:
			if event.IsFatal() {
				return event
			}
			log.Printf("Kafka: %v", event)
		}
	}
}

func applyBalanceUpdate(ctx context.Context, db *sql.DB, message *ckafka.Message) error {
	if message.TopicPartition.Partition != 0 {
		return errors.New("balances topic must have one partition")
	}
	var event balanceUpdated
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return err
	}
	payload := event.Payload
	if event.Name != "BalanceUpdated" || payload.AccountFrom == "" || payload.AccountTo == "" ||
		payload.BalanceFrom == nil || payload.BalanceTo == nil {
		return errors.New("invalid BalanceUpdated event")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Kafka can redeliver an event when the consumer stops between the two commits.
	const upsert = `INSERT INTO balances (account_id, balance, kafka_offset)
		VALUES (?, ?, ?) AS incoming
		ON DUPLICATE KEY UPDATE
			balance = IF(incoming.kafka_offset > balances.kafka_offset, incoming.balance, balances.balance),
			kafka_offset = GREATEST(incoming.kafka_offset, balances.kafka_offset)`
	offset := int64(message.TopicPartition.Offset)
	if _, err := tx.ExecContext(ctx, upsert, payload.AccountFrom, *payload.BalanceFrom, offset); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, upsert, payload.AccountTo, *payload.BalanceTo, offset); err != nil {
		return err
	}
	return tx.Commit()
}
