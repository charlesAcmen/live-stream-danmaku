package ws

// Manager 负责管理所有的 Client
type Manager struct {
	// 注册通道：有新人进来，把 Client 指针丢进去
	Register chan *Client

	// 注销通道：有人断连，把 Client 指针丢进去
	Unregister chan *Client

	// 广播通道：有新消息要转发，把 []byte 丢进去
	Broadcast chan []byte

	// 核心状态：保存所有在线的 Client
	// key 是 Client 指针，value 是 bool (true表示在线)
	Clients map[*Client]bool
}

// NewManager 创建一个大管家
func NewManager() *Manager {
	return &Manager{
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Broadcast:  make(chan []byte),
		Clients:    make(map[*Client]bool),
	}
}

// Start 启动管家的大循环
// 只要程序不倒，这个 Start 就一直转，类似于 C++ 的 EventLoop
func (m *Manager) Start() {
	for {
		// select 是 Go 的精髓：同时监听多个 Channel，哪个有数据处理哪个
		// 相当于 C++ 的 epoll / select
		select {
		case conn := <-m.Register:
			// 有新人加入
			m.Clients[conn] = true

		case conn := <-m.Unregister:
			// 有人离开
			if _, ok := m.Clients[conn]; ok {
				delete(m.Clients, conn)
				close(conn.Send) // 关闭发送通道，防止死锁
			}

		case message := <-m.Broadcast:
			// 有人喊话，遍历所有人，发给他
			for conn := range m.Clients {
				select {
				case conn.Send <- message:
					// 正常发送
				default:
					// 如果发不出去（对方接收缓冲区满了），就踢掉，防止阻塞整个服务器
					// 这是高并发系统的自我保护机制
					close(conn.Send)
					delete(m.Clients, conn)
				}
			}
		}
	}
}
