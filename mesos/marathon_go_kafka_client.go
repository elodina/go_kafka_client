package main

import (
    marathon "github.com/gambol99/go-marathon"
    "os"
    "fmt"
)

func main() {
    marathon_url := "http://192.168.3.5:8080"
    config := marathon.NewDefaultConfig()
    config.URL = marathon_url
    config.LogOutput = os.Stdout
    if client, err := marathon.NewClient(config); err != nil {
        panic(fmt.Sprintf("Failed to create a client for marathon, error: %s", err))
    } else {
        application := marathon.NewDockerApplication()
        application.Name("go-kafka-consumers")
        application.CPU(0.1).Memory(1024).Storage(1024).Count(2)
        application.AddEnv("ZOOKEEPER_CONNECT", "192.168.3.5:2181")

        // add the docker container
        application.Container.Docker.Container("go_kafka_consumers")

        if err := client.CreateApplication(application, true); err != nil {
            fmt.Printf("Failed to create application: %s, error: %s", application, err)
        } else {
            fmt.Printf("Created the application: %s", application)
        }
    }
}