package main

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
)

type Message struct {
	Topic   string `json:"topic"`
	Content string `json:"content"`
	Sender  string `json:"sender"`
}

type Broker struct {
	subscribers  map[string][]net.Conn
	messageStore map[string][]Message
	mu           sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		subscribers:  make(map[string][]net.Conn),
		messageStore: make(map[string][]Message),
	}
}

func (b *Broker) Subscribe(topic string, conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.subscribers[topic] = append(b.subscribers[topic], conn)

	if messages, ok := b.messageStore[topic]; ok {
		for _, msg := range messages {
			if err := json.NewEncoder(conn).Encode(msg); err != nil {
				fmt.Println("error sending stored message:", err)
			}
		}
	}
}

func (b *Broker) Unsubscribe(topic string, conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	conns := b.subscribers[topic]
	for i, c := range conns {
		if c == conn {
			b.subscribers[topic] = append(conns[:i], conns[i+1:]...)
			break
		}
	}
}

func (b *Broker) Publish(msg Message) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.messageStore[msg.Topic] = append(b.messageStore[msg.Topic], msg)

	conns, ok := b.subscribers[msg.Topic]
	if ok {
		for _, conn := range conns {
			if err := json.NewEncoder(conn).Encode(msg); err != nil {
				fmt.Println("error sending message:", err)
			}
		}
	}
	fmt.Printf("logged message: [%s] %s: %s\n", msg.Topic, msg.Sender, msg.Content)
}

func (b *Broker) HandleConnection(conn net.Conn) {
	defer conn.Close()
	var action struct {
		Type    string `json:"type"`
		Topic   string `json:"topic,omitempty"`
		Content string `json:"content,omitempty"`
		Sender  string `json:"sender,omitempty"`
	}

	for {
		if err := json.NewDecoder(conn).Decode(&action); err != nil {
			fmt.Println("error decoding:", err)
			return
		}

		switch action.Type {
		case "subscribe":
			b.Subscribe(action.Topic, conn)
		case "unsubscribe":
			b.Unsubscribe(action.Topic, conn)
		case "publish":
			msg := Message{Topic: action.Topic, Content: action.Content, Sender: action.Sender}
			b.Publish(msg)
		}
	}
}

func main() {
	broker := NewBroker()
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("error starting server:", err)
		return
	}
	defer listener.Close()

	fmt.Println("broker listening on :8080")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("error accepting connection:", err)
			continue
		}
		go broker.HandleConnection(conn)
	}
}
