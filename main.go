package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
)

type EmailRequest struct {
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

func sendEmailHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	var req EmailRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	err = SendEmail(r.Context(), req, redisSvc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintln(w, "Email successfully sent")
}

func main() {
	http.HandleFunc("/send-email", sendEmailHandler)

	fmt.Println("Server running on :9000")
	log.Fatal(http.ListenAndServe(":9000", nil))
}
