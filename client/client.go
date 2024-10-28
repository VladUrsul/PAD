package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

type Message struct {
	Topic   string `json:"topic"`
	Content string `json:"content"`
	Sender  string `json:"sender"`
}

func main() {
	var name string
	fmt.Println("enter your name:")
	fmt.Scanln(&name)

	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println("error connecting to broker:", err)
		return
	}
	defer conn.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			var msg Message
			if err := json.NewDecoder(conn).Decode(&msg); err == nil {
				fmt.Printf("received message on topic '%s' from %s: %s\n", msg.Topic, msg.Sender, msg.Content)
			}
		}
	}()

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("enter command (subscribe, unsubscribe, publish <topic> <message>, exit):")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		switch {
		case input == "subscribe":
			fmt.Println("enter topic to subscribe:")
			topic, _ := reader.ReadString('\n')
			topic = strings.TrimSpace(topic)
			action := map[string]string{"type": "subscribe", "topic": topic}
			json.NewEncoder(conn).Encode(action)

		case input == "unsubscribe":
			fmt.Println("enter topic to unsubscribe:")
			topic, _ := reader.ReadString('\n')
			topic = strings.TrimSpace(topic)
			action := map[string]string{"type": "unsubscribe", "topic": topic}
			json.NewEncoder(conn).Encode(action)

		case strings.HasPrefix(input, "publish"):
			parts := strings.Fields(input)
			if len(parts) < 3 {
				fmt.Println("invalid publish command. Format: publish <topic> <message>")
				continue
			}
			topic := parts[1]
			content := strings.Join(parts[2:], " ")

			action := map[string]interface{}{
				"type":    "publish",
				"topic":   topic,
				"content": content,
				"sender":  name,
			}
			json.NewEncoder(conn).Encode(action)

		case input == "exit":
			fmt.Println("exiting...")
			return
		}
	}
}
