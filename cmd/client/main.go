package main

import (
	"bufio"         // read input from stdin
	"encoding/json" //serilize and deserilize danmaku messages
	"flag"          // command line arguments
	"fmt"
	"log"       // log to stderr
	"net/url"   // parse url
	"os"        // operating system functions
	"os/signal" // handle signals
	"time"

	"github.com/gorilla/websocket" // websocket library
)

// Helper struct to parse incoming JSON messages for display
type IncomingMsg struct {
	Username string `json:"username"`
	Content  string `json:"content"`
}

func main() {
	// Define command-line flag for the target server port.
	port := flag.String("port", "8080", "server port to connect to")
	uid := flag.String("uid", "", "user id (random if empty)")
	room := flag.String("room", "1001", "room id")
	flag.Parse()

	//Generate random user if not provided
	if *uid == "" {
		*uid = fmt.Sprintf("User-%d", time.Now().UnixNano())
	}

	log.Printf("Connecting as %s to Room %s...", *uid, *room)

	// 1. server address
	//Scheme: protocol, Host: server address, Path: route
	u := url.URL{Scheme: "ws", Host: "localhost:" + *port, Path: "/ws"}
	q := u.Query()
	//?uid=...
	q.Set("uid", *uid)
	//&name=...
	q.Set("name", *uid) // Use ID as name for simplicity
	//&room=...
	q.Set("room", *room)
	u.RawQuery = q.Encode()

	//ws://localhost:8080/ws?uid=1001&name=1001&room=1001
	log.Printf("[CLIENT]Connecting to: %s", u.String())

	// 2. initiate connection (handshake)
	// Dial is like making a phone call, send a HTTP request with a header with upgrade:websocket
	// returns a conn (connection object) when connected
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("[CLIENT]connecting failed", err)
	}
	defer c.Close()

	// 3. start a goroutine to listen for messages from server
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("[CLIENT]disconnected when receiving message", err)
				return
			}
			// Parse the JSON message to display nicely
			var msgData IncomingMsg
			json.Unmarshal(message, &msgData)
			log.Printf("[Live Chat][%s]: %s\n", msgData.Username, msgData.Content)
			// print received danmaku
			// log.Printf("[Live Chat]: %s\n", message)
			log.Printf("[CLIENT]Please input:")
		}
	}()

	// 4. listen on interrupt signal (Ctrl+C)
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	log.Println("[CLIENT]Connected Successfully!Now let's chat")
	log.Println("[CLIENT]Please input: ")
	scanner := bufio.NewScanner(os.Stdin)

	// 5.start a goroutine to handle keyboard input, prevent select from blocking
	go func() {
		for scanner.Scan() {
			//send raw danmaku text,the server will wrap it into JSON with userid etc
			text := scanner.Text()
			err := c.WriteMessage(websocket.TextMessage, []byte(text))
			if err != nil {
				log.Println("[CLIENT]Failed to send message:", err)
				return
			}
		}
	}()

	// 6. block main routine, wait for disconnection or interruption
	for {
		select {
		case <-done:
			return
		case <-interrupt:
			// received Ctrl+C, send close message to server
			log.Println("[CLIENT]Closing connection...")
			err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				log.Println("[CLIENT]Failed to Close connection:", err)
			}
			return
		}
	}
}
