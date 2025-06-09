package main

import (
	"fmt"

	gedis_client "github.com/AAVision/gedis/gedis-client"
)

func main() {
	client, err := gedis_client.NewClient("localhost:9999")
	if err != nil {
		fmt.Println("Failed to connect:", err)
		return
	}
	defer client.Close()

	response, err := client.Ping()
	if err != nil {
		fmt.Println("Ping failed:", err)
		return
	}
	fmt.Println("Server response:", response)

	err = client.Set("greeting", "Hello, AA!")
	if err != nil {
		fmt.Println("SET failed:", err)
		return
	}

	value, err := client.GetString("greeting")
	if err != nil {
		fmt.Println("GET failed:", err)
		return
	}
	fmt.Println("Greeting:", value)

}
