package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// MovieDTO
type MovieDTO struct {
	Title    string   `json:"originalTitle"`
	Runtime  int      `json:"runtimeMinutes"`
	YearMade int      `json:"startYear"`
	Genres   []string `json:"genres"`
}

var moviePointer = -1;

// to lazy to pull from file
var movies = []MovieDTO{
	{"The Matrix", 136, 1999, []string{"Action", "Sci-Fi"}},
	{"Inception", 148, 2010, []string{"Action", "Sci-Fi", "Thriller"}},
	{"The Godfather", 175, 1972, []string{"Crime", "Drama"}},
	{"Spirited Away", 125, 2001, []string{"Animation", "Adventure", "Family"}},
}

// WebSocket upgrade, allow all.
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// get a array of movies
func getMoviesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(movies); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// get a single move wrapping the array.
func getMovieHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if moviePointer < len(movies) {
		moviePointer ++
	}
	if moviePointer == len(movies) {
		moviePointer = 0
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(movies[moviePointer]); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// WebSocket handler
func wsHandler(w http.ResponseWriter, r *http.Request) {

	conn, err := upgrader.Upgrade(w, r, nil)

	if err != nil {
		log.Println("WebSocket Upgrade Error:", err)
		return
	}
	defer conn.Close()

	log.Println("Client connected via WebSocket")

	// Infinite loop.
	i := 0
	for {
		movie := movies[i%len(movies)]
		err := conn.WriteJSON(movie)
		if err != nil {
			log.Println("WebSocket Write Error (Client likely disconnected):", err)
			break
		}

		time.Sleep(1 * time.Second)
		i++
	}
}

func main() {
	http.HandleFunc("/movies", getMoviesHandler)
	http.HandleFunc("/movie", getMovieHandler)
	http.HandleFunc("/ws", wsHandler)

	log.Println("Server starting on port 8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("Server Error:", err)
	}
}