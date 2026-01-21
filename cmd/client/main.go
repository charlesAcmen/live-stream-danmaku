package main

import (
	"bufio"         // read input from stdin
	"encoding/json" //serilize and deserilize danmaku messages
	"flag"          // command line arguments
	"fmt"
	"net/url"   // parse url
	"os"        // operating system functions
	"os/signal" // handle signals
	"time"

	"github.com/charlesAcmen/livestream-danmaku/internal/logger"
	"github.com/charlesAcmen/livestream-danmaku/internal/model"
	"github.com/gorilla/websocket" // websocket library
	"go.uber.org/zap"
)

func sendPacket(c *websocket.Conn, msgType int, data interface{}) {
	// 1.data to JSON bytes
	// e.g. {"content": "hello"}
	dataBytes, _ := json.Marshal(data)

	// 2. construct websocket packet
	packet := model.WsPacket{
		Type: msgType,
		//json.RawMessage
		Data: dataBytes,
	}
	if err := c.WriteJSON(packet); err != nil {
		logger.Log.Error("[CLIENT]Failed to send", zap.Error(err))
	}
}

func main() {
	logger.InitLogger("dev")
	defer logger.Sync()

	// Define command-line flag for the target server port.
	port := flag.String("port", "8080", "server port to connect to")
	uid := flag.String("uid", "", "user id (random if empty)")
	room := flag.String("room", "1001", "room id")
	flag.Parse()

	//Generate random user if not provided
	if *uid == "" {
		*uid = fmt.Sprintf("User-%d", time.Now().UnixNano())
	}

	logger.Log.Info("Connecting...", zap.String("uid", *uid), zap.String("room", *room))

	// 1. server address
	//Scheme: protocol, Host: server address, Path: route
	u := url.URL{Scheme: "ws", Host: "127.0.0.1:" + *port, Path: "/ws"}
	q := u.Query()
	//?uid=...
	q.Set("uid", *uid)
	//&name=...
	q.Set("name", *uid) // Use ID as name for simplicity
	//&room=...
	q.Set("room", *room)
	u.RawQuery = q.Encode()

	//ws://localhost:8080/ws?uid=1001&name=1001&room=1001
	logger.Log.Info("[CLIENT]Connecting to...", zap.String("url", u.String()))

	// 2. initiate connection (handshake)
	// Dial is like making a phone call, send a HTTP request with a header with upgrade:websocket
	// returns a conn (connection object) when connected
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		logger.Log.Fatal("[CLIENT]connecting failed", zap.Error(err))
	}
	defer c.Close()

	// 3. start a goroutine to listen for messages from server
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				logger.Log.Error("[CLIENT]disconnected when receiving message", zap.Error(err))
				return
			}

			// 1. unwrap wsPacket
			var packet model.WsPacket
			if err := json.Unmarshal(message, &packet); err != nil {
				continue
			}
			// 2.unmarshal based on type
			switch packet.Type {

			case model.TypeDanmaku:
				var danmaku model.DanmakuMessage
				json.Unmarshal(packet.Data, &danmaku)
				logger.Log.Info("[Live Chat]", zap.String("content", danmaku.Content))

			case model.TypeStats:
				var stats model.StatsData
				json.Unmarshal(packet.Data, &stats)
				logger.Log.Info("[STATS]", zap.Int("Online", int(stats.Online)), zap.Int("Likes", int(stats.Likes)))
			}

			logger.Log.Info("[CLIENT]Please input:")
		}
	}()

	// 4. listen on interrupt signal (Ctrl+C)
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	logger.Log.Info("[CLIENT]Connected Successfully! Now let's chat")
	logger.Log.Info("[CLIENT]Please input:")
	scanner := bufio.NewScanner(os.Stdin)

	// 5.start a goroutine to handle keyboard input, prevent select from blocking
	go func() {
		for scanner.Scan() {
			//send raw danmaku text,the server will wrap it into JSON with userid etc
			text := scanner.Text()
			if text == "1" {
				// ActionLike (103)
				// data:empty
				sendPacket(c, model.ActionLike, nil)
				logger.Log.Info("已点赞 ❤️")

			} else {
				// === Send Danmaku ===
				// type:TypeDanmaku (101)
				// notice：client only needs to fill in Content，other info including
				// ID/Time is filled by server
				msg := model.DanmakuMessage{
					Content: text,
				}
				sendPacket(c, model.TypeDanmaku, msg)
			}
			// err := c.WriteMessage(websocket.TextMessage, []byte(text))
			// if err != nil {
			// 	logger.Log.Println("[CLIENT]Failed to send message:", err)
			// 	return
			// }
		}
	}()

	// 6. block main routine, wait for disconnection or interruption
	for {
		select {
		case <-done:
			return
		case <-interrupt:
			// received Ctrl+C, send close message to server
			logger.Log.Info("[CLIENT]Closing connection...")
			err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				logger.Log.Info("[CLIENT]Failed to Close connection", zap.Error(err))
			}
			return
		}
	}
}
