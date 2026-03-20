package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"time"
)

func main() {
	 rand.Seed(time.Now().UnixNano())
	 
    port := os.Getenv("PORT")
    if port == "" {
        port = "8000"
    }

		http.HandleFunc("/health", healthCheck)
		http.HandleFunc("/api/movies", handleMoviesProxy)
		http.HandleFunc("/api/users", handleMonolithProxy)

    log.Printf("starting proxy service on port %s", port)

    err := http.ListenAndServe(":"+port, nil)
    if err != nil {
        log.Fatal(err)
    }
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status": true}`))
}

func handleMoviesProxy(w http.ResponseWriter, r *http.Request) {
	moviesServiceURL := getMoviesTargetURL()

	targetURL, err := url.Parse(moviesServiceURL)
	if err != nil {
			http.Error(w, "invalid movies service url", http.StatusInternalServerError)
			return
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.ServeHTTP(w, r)
}

func handleMonolithProxy(w http.ResponseWriter, r *http.Request) {
    monolithURL := os.Getenv("MONOLITH_URL")
    if monolithURL == "" {
        monolithURL = "http://localhost:8080"
    }

    targetURL, err := url.Parse(monolithURL)
    if err != nil {
        http.Error(w, "invalid monolith url", http.StatusInternalServerError)
        return
    }

    proxy := httputil.NewSingleHostReverseProxy(targetURL)
    proxy.ServeHTTP(w, r)
}

func getMoviesTargetURL() string {
    monolithURL := os.Getenv("MONOLITH_URL")
    if monolithURL == "" {
        monolithURL = "http://localhost:8080"
    }

    moviesServiceURL := os.Getenv("MOVIES_SERVICE_URL")
    if moviesServiceURL == "" {
        moviesServiceURL = "http://localhost:8081"
    }

    gradualMigration := os.Getenv("GRADUAL_MIGRATION")
     if gradualMigration != "true" {
        return monolithURL
    }

    migrationPercentText := os.Getenv("MOVIES_MIGRATION_PERCENT")
    if migrationPercentText == "" {
        migrationPercentText = "100"
    }

    migrationPercent, err := strconv.Atoi(migrationPercentText)
    if err != nil {
        return monolithURL
    }

    if migrationPercent <= 0 {
        return monolithURL
    }

    if migrationPercent >= 100 {
        return moviesServiceURL
    }

    randomNumber := rand.Intn(100)

    if randomNumber < migrationPercent {
        return moviesServiceURL
    }

    return monolithURL
}