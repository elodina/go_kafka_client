package main

import (
    marathon "github.com/gambol99/go-marathon"
    "os"
    "fmt"
)

func main() {
    marathon_url := "http://192.168.3.5:8000"
    config := marathon.NewDefaultConfig()
    config.URL = marathon_url
    config.LogOutput = os.Stdout
    if client, err := marathon.NewClient(config); err != nil {
        panic(fmt.Sprintf("Failed to create a client for marathon, error: %s", err))
    } else {
        application := marathon.NewDockerApplication()
        application.Name("/go-kafka-consumers")
        application.CPU(0.5).Memory(256).Storage(256).Count(1)
        application.AddEnv("ZOOKEEPER_CONNECT", "192.168.3.5:2181")
        application.AddEnv("TOPIC", "testing1")

        // add the docker container
        application.Container.Docker.Container("localhost:5000/go_kafka_consumers:latest")

        if err := client.CreateApplication(application, true); err != nil {
            fmt.Printf("Failed to create application: %s, error: %s", application, err)
        } else {
            fmt.Printf("Created the application: %s", application)
        }
    }
}