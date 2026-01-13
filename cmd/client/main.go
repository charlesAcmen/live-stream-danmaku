package main

import (
	"bufio"     // read input from stdin
	"flag"      // command line arguments
	"log"       // log to stderr
	"net/url"   // parse url
	"os"        // operating system functions
	"os/signal" // handle signals

	"github.com/gorilla/websocket" // websocket library
)

func main() {
	// Define command-line flag for the target server port.
	port := flag.String("port", "8080", "server port to connect to")
	flag.Parse()

	// 1. server address
	//Scheme: protocol, Host: server address, Path: route
	u := url.URL{Scheme: "ws", Host: "localhost:" + *port, Path: "/ws"}
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
			// print received danmaku
			log.Printf("[Live Chat]: %s\n", message)
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
