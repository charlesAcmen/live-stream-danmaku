package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/charlesAcmen/livestream-danmaku/internal/model"
)

// Metrics (Atomic counters for thread-safety)
var (
	connectedCount int64 // Total currently connected clients
	msgSentCount   int64 // Total messages sent (Cumulative)
	msgRecvCount   int64 // Total messages received (Cumulative)
	errorCount     int64 // Total errors occurred
)

// Target servers to simulate client-side load balancing.
// The benchmark tool will distribute connections evenly among these hosts.
var targetHosts = []string{
	"localhost:8081",
	"localhost:8082",
}

func main() {
	// 1. Parse command line flags
	clients := flag.Int("c", 1000, "Number of concurrent clients")
	rate := flag.Duration("r", 5*time.Second, "Message sending interval per client")
	flag.Parse()

	// 2. Initialize Logger (Development mode for colored output)
	logger.InitLogger("dev")
	defer logger.Sync()

	logger.Log.Info("[BENCHMARK] Starting...",
		zap.Int("clients", *clients),
		zap.Duration("rate", *rate),
		zap.Strings("targets", targetHosts),
	)

	// 3. Start Monitor Goroutine (Prints stats every 1 second)
	go monitor()

	// 4. Start Clients
	// Use a WaitGroup to wait for all clients (though we likely run until Ctrl+C)
	var wg sync.WaitGroup
	wg.Add(*clients)

	// Control the startup rate to avoid "connection refused" or OS limits
	// Start 100 clients per second
	rampUpTicker := time.NewTicker(10 * time.Millisecond)
	defer rampUpTicker.Stop()

	for i := 0; i < *clients; i++ {
		<-rampUpTicker.C // Wait a bit before starting next client

		go func(id int) {
			defer wg.Done()

			// Round-Robin Load Balancing
			// ID 0 -> 8081, ID 1 -> 8082, ID 2 -> 8081...
			host := targetHosts[id%len(targetHosts)]

			runBot(host, id, *rate)
		}(i)
	}

	// 5. Handle Shutdown Signal (Ctrl+C)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Log.Info("[BENCHMARK] Stopping...")
}

// monitor prints the system throughput every second.
func monitor() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastSent, lastRecv int64

	for range ticker.C {
		currConn := atomic.LoadInt64(&connectedCount)
		currSent := atomic.LoadInt64(&msgSentCount)
		currRecv := atomic.LoadInt64(&msgRecvCount)
		currErr := atomic.LoadInt64(&errorCount)

		// Calculate QPS (Queries Per Second)
		sentRate := currSent - lastSent
		recvRate := currRecv - lastRecv

		lastSent = currSent
		lastRecv = currRecv

		// Use fmt for stats to keep it distinct from zap logs
		fmt.Printf("\r[STATS] Conns: %d | Sent: %d/s | Recv: %d/s | Errs: %d",
			currConn, sentRate, recvRate, currErr)
	}
}

// runBot simulates a single user behavior.
func runBot(host string, id int, interval time.Duration) {
	// 1. Build WebSocket URL with authentication params
	u := url.URL{Scheme: "ws", Host: host, Path: "/ws"}
	q := u.Query()
	q.Set("uid", fmt.Sprintf("bot-%d", id))
	q.Set("name", "benchmark-bot")
	q.Set("room", "1001")
	u.RawQuery = q.Encode()

	// 2. Connect to Server
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		atomic.AddInt64(&errorCount, 1)
		// logger.Log.Error("Dial error", zap.Error(err)) // Too noisy for benchmark
		return
	}
	defer c.Close()

	// Update counter
	atomic.AddInt64(&connectedCount, 1)
	defer atomic.AddInt64(&connectedCount, -1)

	// 3. Start Read Loop (Consume and discard messages to keep connection alive)
	go func() {
		for {
			_, _, err := c.ReadMessage()
			if err != nil {
				return
			}
			atomic.AddInt64(&msgRecvCount, 1)
		}
	}()

	// 4. Start Write Loop (Send Danmaku periodically)
	// Add some jitter to avoid all bots sending at the exact same millisecond
	time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		sendDanmaku(c)
	}
}

// sendDanmaku constructs a valid WsPacket and sends it.
func sendDanmaku(c *websocket.Conn) {
	// 1. Create the Payload (The content)
	msgContent := model.DanmakuMessage{
		Content: fmt.Sprintf("bench-msg-%d", time.Now().UnixNano()),
	}
	dataBytes, _ := json.Marshal(msgContent)

	// 2. Wrap in Envelope (WsPacket)
	packet := model.WsPacket{
		Type: model.TypeDanmaku,
		Data: dataBytes,
	}

	// 3. Serialize and Send
	if err := c.WriteJSON(packet); err != nil {
		atomic.AddInt64(&errorCount, 1)
		return
	}

	atomic.AddInt64(&msgSentCount, 1)
}
