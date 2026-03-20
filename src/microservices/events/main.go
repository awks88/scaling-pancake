package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

var writer *kafka.Writer
var reader *kafka.Reader

type MovieEvent struct {
    MovieID     int       `json:"movie_id"`
    Title       string    `json:"title"`
    Action      string    `json:"action"`
    UserID      int       `json:"user_id"`
    Rating      float64   `json:"rating"`
    Genres      []string  `json:"genres"`
    Description string    `json:"description"`
    Timestamp   time.Time `json:"timestamp"`
}

type Event struct {
    ID        string      `json:"id"`
    Type      string      `json:"type"`
    Timestamp time.Time   `json:"timestamp"`
    Payload   interface{} `json:"payload"`
}

type EventResponse struct {
    Status    string `json:"status"`
    Partition int    `json:"partition"`
    Offset    int    `json:"offset"`
    Event     Event  `json:"event"`
}

type UserEvent struct {
    UserID    int       `json:"user_id"`
    Username  string    `json:"username"`
    Email     string    `json:"email"`
    Action    string    `json:"action"`
    Timestamp time.Time `json:"timestamp"`
}

type PaymentEvent struct {
    PaymentID  int       `json:"payment_id"`
    UserID     int       `json:"user_id"`
    Amount     float64   `json:"amount"`
    Status     string    `json:"status"`
    Timestamp  time.Time `json:"timestamp"`
    MethodType string    `json:"method_type"`
}

func main() {

		initKafka()
		defer writer.Close()

		initKafkaReader()
		defer reader.Close()

		go consumeMovieEvents()

    port := os.Getenv("PORT")
    if port == "" {
        port = "8082"
    }

		http.HandleFunc("/api/events/health", healthCheck)
		http.HandleFunc("/api/events/movie", handleMovieEvent)
		http.HandleFunc("/api/events/user", handleUserEvent)
		http.HandleFunc("/api/events/payment", handlePaymentEvent)

    log.Printf("starting events service on port %s", port)

    err := http.ListenAndServe(":"+port, nil)
    if err != nil {
        log.Fatal(err)
    }
}

func initKafka() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	writer = &kafka.Writer{
		Addr:     kafka.TCP(brokers),
		Topic:    "movie-events",
		Balancer: &kafka.LeastBytes{},
	}
}

func initKafkaReader() {
    brokers := os.Getenv("KAFKA_BROKERS")
    if brokers == "" {
        brokers = "localhost:9092"
    }

    reader = kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{brokers},
        Topic:   "movie-events",
        GroupID: "events-service-group",
    })
}

func consumeMovieEvents() {
    for {
        message, err := reader.ReadMessage(context.Background())
        if err != nil {
            log.Printf("failed to read message from kafka: %v", err)
            time.Sleep(2 * time.Second)
            continue
        }

        log.Printf("received movie event: %s", string(message.Value))
    }
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status": true}`))
}


func handleMovieEvent(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
        return
    }

    var event MovieEvent
		
		err := json.NewDecoder(r.Body).Decode(&event)
		if err != nil {
				http.Error(w, "invalid request body", http.StatusBadRequest)
				return
		}

     if event.MovieID == 0 || event.Title == "" || event.Action == "" {
        http.Error(w, "movie_id, title and action are required", http.StatusBadRequest)
        return
    }

    event.Timestamp = time.Now()

    messageBytes, err := json.Marshal(event)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    err = writer.WriteMessages(
        context.Background(),
        kafka.Message{
            Key:   []byte("movie-" + strconv.Itoa(event.MovieID)),
            Value: messageBytes,
        },
    )
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

		apiEvent := Event{
				ID:        "movie-" + strconv.Itoa(event.MovieID) + "-" + event.Action,
				Type:      "movie",
				Timestamp: event.Timestamp,
				Payload:   event,
		}

		response := EventResponse{
				Status:    "success",
				Partition: 0,
				Offset:    0,
				Event:     apiEvent,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)

		err = json.NewEncoder(w).Encode(response)
		if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
		}
}

func handleUserEvent(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
        return
    }

    var event UserEvent

    err := json.NewDecoder(r.Body).Decode(&event)
    if err != nil {
        http.Error(w, "invalid request body", http.StatusBadRequest)
        return
    }

    if event.UserID == 0 || event.Action == "" {
        http.Error(w, "user_id and action are required", http.StatusBadRequest)
        return
    }

    event.Timestamp = time.Now()

    messageBytes, err := json.Marshal(event)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    err = writer.WriteMessages(
        context.Background(),
        kafka.Message{
            Key:   []byte("user-" + strconv.Itoa(event.UserID)),
            Value: messageBytes,
        },
    )
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    apiEvent := Event{
        ID:        "user-" + strconv.Itoa(event.UserID) + "-" + event.Action,
        Type:      "user",
        Timestamp: event.Timestamp,
        Payload:   event,
    }

    response := EventResponse{
        Status:    "success",
        Partition: 0,
        Offset:    0,
        Event:     apiEvent,
    }

    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusCreated)

    err = json.NewEncoder(w).Encode(response)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
    }
}

func handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
        return
    }

    var event PaymentEvent

    err := json.NewDecoder(r.Body).Decode(&event)
    if err != nil {
        http.Error(w, "invalid request body", http.StatusBadRequest)
        return
    }

    if event.PaymentID == 0 || event.UserID == 0 || event.Amount == 0 || event.Status == "" {
        http.Error(w, "payment_id, user_id, amount and status are required", http.StatusBadRequest)
        return
    }

    event.Timestamp = time.Now()

    messageBytes, err := json.Marshal(event)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    err = writer.WriteMessages(
        context.Background(),
        kafka.Message{
            Key:   []byte("payment-" + strconv.Itoa(event.PaymentID)),
            Value: messageBytes,
        },
    )
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    apiEvent := Event{
        ID:        "payment-" + strconv.Itoa(event.PaymentID) + "-" + event.Status,
        Type:      "payment",
        Timestamp: event.Timestamp,
        Payload:   event,
    }

    response := EventResponse{
        Status:    "success",
        Partition: 0,
        Offset:    0,
        Event:     apiEvent,
    }

    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusCreated)

    err = json.NewEncoder(w).Encode(response)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
    }
}