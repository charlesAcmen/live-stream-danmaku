package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"sync"        //sync.WaitGroup wait for all done
	"sync/atomic" //data race,memory visibility,better than sync.Mutex

	//so fast because of utilizing CPU hardware insturctions
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/charlesAcmen/livestream-danmaku/internal/model"
)

// Metrics (Atomic counters for thread-safety)
var (
	connectedCount    int64 // Total currently connected clients
	msgSentCount      int64 // Total messages sent (Cumulative)
	msgRecvCount      int64 // Total messages received (Cumulative)
	errorCount        int64 // Total errors occurred
	expectedRecvCount int64 // Sum of (1 sent * room_size) at the moment of sending
)

// Target servers to simulate client-side load balancing.
// The benchmark tool will distribute connections evenly among these hosts.
var targetHosts = []string{
	"localhost:8081",
	"localhost:8082",
	// "localhost:8083",
	// "localhost:8084",
}

const (
	TotalClients       = 15000                // Total concurrent connections
	ActiveUserRatio    = 0.003                // users are "talkers", are "lurkers" (listeners)
	AvgMessageInterval = 1 * time.Second      // On average, a talker sends a message every 10 seconds
	RoomCapacity       = 100000               // Max users per room (to simulate multiple rooms)
	RampUpSpeed        = 2 * time.Millisecond // Connection speed limit

	ReportInterval = 10 * time.Second // Period for long-term stats
)

func main() {
	// 1. Parse command line flags
	// maximum capacity is 56,460 because of non-infinite number of ports on this laptop
	clients := flag.Int("c", TotalClients, "Number of concurrent clients")
	rate := flag.Duration("r", AvgMessageInterval, "Danmaku sending interval per client")
	flag.Parse()

	// 2. Initialize Logger (Development mode for colored output)
	logger.InitLogger("dev")
	defer logger.Sync()

	logger.Log.Info("[BENCHMARK] Starting...",
		zap.Int("total_conns", *clients),
		zap.Float64("active_ratio", ActiveUserRatio), // Percentage of users who actually speak
		zap.Duration("avg_interval", *rate),          // Base interval before jitter
		zap.Int("room_cap", RoomCapacity),            // Users per room
		zap.Duration("report_interval", ReportInterval),
		zap.Strings("target_servers", targetHosts),
	)

	// 3. Start Monitor Goroutine (Prints stats every 1 second)
	go monitor()

	// 4. Start Clients
	// Use a WaitGroup to wait for all clients (though we likely run until Ctrl+C)
	var wg sync.WaitGroup
	wg.Add(*clients)

	// Control the startup rate to avoid "connection refused" or OS limits
	rampUpTicker := time.NewTicker(RampUpSpeed)
	defer rampUpTicker.Stop()

	for i := 0; i < *clients; i++ {
		<-rampUpTicker.C // Wait a bit before starting next client
		go func(id int) {
			//decr counter by 1
			defer wg.Done()

			// Round-Robin Load Balancing
			// ID 0 -> 8081, ID 1 -> 8082, ID 2 -> 8081...
			host := targetHosts[id%len(targetHosts)]

			runBot(host, id, *rate)
		}(i)
	}

	wg.Wait()

	// 5. Handle Shutdown Signal (Ctrl+C)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Log.Info("[BENCHMARK] Stopping...")
}

// monitor prints the system throughput every second.
func monitor() {
	logger.Log.Info(
		"[BENCHMARK] The following periodic statistics are accurate only when running with a single room.")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	//every second
	var lastSent, lastRecv int64
	var (
		winStartSent int64
		winStartRecv int64
		winStartExp  int64
		winStartErr  int64
	)
	secondsCounter := 0
	for range ticker.C {
		currConn := atomic.LoadInt64(&connectedCount)
		currSent := atomic.LoadInt64(&msgSentCount)
		currRecv := atomic.LoadInt64(&msgRecvCount)
		currExp := atomic.LoadInt64(&expectedRecvCount)
		currErr := atomic.LoadInt64(&errorCount)

		// Calculate QPS (Queries Per Second)
		sentRate := currSent - lastSent // Messages sent per second
		recvRate := currRecv - lastRecv // Messages received per second
		lastSent, lastRecv = currSent, currRecv

		lag := currExp - currRecv // Current total outstanding messages (Expected - Received)
		var totalLossRate float64 // Cumulative loss rate since benchmark start
		if currExp > 0 {
			totalLossRate = float64(currExp-currRecv) / float64(currExp) * 100
		}

		logger.Log.Info("[STATS]",
			zap.Int64("Conns", currConn),
			zap.Int64("Sent/s", sentRate),
			zap.Int64("Recv/s", recvRate),
			zap.Int64("CurrentLag", lag), // "In-flight" or "backlog" messages
			zap.Int64("Errs", currErr),
			zap.String("TotalLoss%", fmt.Sprintf("%.2f%%", totalLossRate)),
		)

		secondsCounter++
		if secondsCounter >= int(ReportInterval.Seconds()) {
			// Calculate deltas for the current reporting window
			winSent := currSent - winStartSent // Source messages sent in this window
			winExp := currExp - winStartExp    // Theoretical total messages expected in this window
			winRecv := currRecv - winStartRecv // Actual total messages received in this window
			winErr := currErr - winStartErr    // Errors occurred in this window
			// Net loss/gain in messages for this window
			// A negative value means more messages were received than expected in this window (catching up on lag)
			winNetLoss := winExp - winRecv
			var winLossRate float64 // Loss rate specific to this window
			if winExp > 0 {
				winLossRate = float64(winNetLoss) / float64(winExp) * 100
			}
			// Average received QPS for this window
			duration := float64(secondsCounter) // The actual duration of this window in seconds
			avgRecvQPS := float64(winRecv) / duration

			logger.Log.Info("[PERIODIC REPORT]",
				zap.Int64("Win_SourceSent", winSent),     // Source messages sent by bots
				zap.Int64("Win_Expected", winExp),        // Theoretical total fan-out messages
				zap.Int64("Win_ActualReceived", winRecv), // Total messages received by all bots
				zap.Int64("Win_NetLoss", winNetLoss),     // Net loss in this window (can be negative if catching up)
				zap.String("Win_LossRate", fmt.Sprintf("%.2f%%", winLossRate)),
				zap.Int64("Win_AvgRecvQPS", int64(avgRecvQPS)),
				zap.Int64("Win_Errors", winErr),
				zap.Int64("CurrentLag", lag), // Snapshot of current backlog/in-flight messages
			)
			// Reset window start values for the next period
			winStartSent, winStartRecv, winStartExp, winStartErr = currSent, currRecv, currExp, currErr
			secondsCounter = 0
		}
	}
}

// runBot simulates a single user behavior.
func runBot(host string, id int, interval time.Duration) {
	// 1. Distribute bots into different rooms based on RoomCapacity
	roomID := fmt.Sprintf("room_%d", id/RoomCapacity)

	// Build WebSocket URL with authentication params
	u := url.URL{Scheme: "ws", Host: host, Path: "/ws"}
	q := u.Query()
	q.Set("uid", fmt.Sprintf("%d", id))
	q.Set("name", fmt.Sprintf("bot-%d", id))
	q.Set("room", roomID)
	u.RawQuery = q.Encode()

	// 2. Connect to Server
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		atomic.AddInt64(&errorCount, 1)
		logger.Log.Error("Dial error", zap.Error(err)) // Too noisy for benchmark
		return
	}

	// Update counter
	atomic.AddInt64(&connectedCount, 1)

	defer func() {
		c.Close()
		atomic.AddInt64(&connectedCount, -1)
	}()

	//exit with channel assist
	done := make(chan struct{})

	// 3. Start Read Loop (Consume and discard messages to keep connection alive)
	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				atomic.AddInt64(&errorCount, 1)
				logger.Log.Warn("[BENCHMARK] Read loop exited due to error",
					zap.Int("bot_id", id),
					zap.Error(err))
				c.Close()
				return
			}
			var packet model.WsPacket
			if err := json.Unmarshal(message, &packet); err == nil {
				if packet.Type == model.TypeDanmaku {
					//more accurate loss calculation
					atomic.AddInt64(&msgRecvCount, 1)
				}
			}
			// atomic.AddInt64(&msgRecvCount, 1)
		}
	}()

	// 3. Realistic Behavior Simulation
	// Only a subset of users (ActiveUserRatio) will send messages.
	isTalker := rand.Float64() < ActiveUserRatio
	if !isTalker {
		// Lurker: Just stay online and listen (the read loop handles it)
		<-done
		return
	}
	// Talker: Send messages with Jitter
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		// Calculate a sleep duration centered around 'interval' with jitter
		// Factor (0.5 + rand.Float64()) gives a range of 0.5 to 1.5, averaging 1.0.
		// So, the average send interval will be 'interval'.
		sleepDuration := time.Duration(float64(interval) * (0.5 + rand.Float64()))
		select {
		case <-done:
			return
		case <-time.After(sleepDuration): // Wait for the calculated sleep duration
			sendDanmaku(c, roomID)
		}
	}
	// Start Write Loop (Send Danmaku periodically)
	// Add some jitter to avoid all bots sending at the exact same millisecond
	// time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
}

// sendDanmaku constructs a valid WsPacket and sends it.
func sendDanmaku(c *websocket.Conn, roomID string) {
	// 1. Create the Payload (The content)
	msgContent := model.DanmakuMessage{
		Content: fmt.Sprintf("bench-msg-%d", rand.Intn(1000)),
	}
	dataBytes, _ := json.Marshal(msgContent)

	// 2. Wrap in Envelope (WsPacket)
	packet := model.WsPacket{
		Type:   model.TypeDanmaku,
		RoomID: roomID,
		Data:   dataBytes,
	}

	// 3. Serialize and Send
	if err := c.WriteJSON(packet); err != nil {
		atomic.AddInt64(&errorCount, 1)
		logger.Log.Warn("[BENCHMARK] Failed to send danmaku", zap.Error(err))
		return
	}

	atomic.AddInt64(&msgSentCount, 1)
	atomic.AddInt64(&expectedRecvCount, atomic.LoadInt64(&connectedCount))
}
