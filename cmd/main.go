package main

import (
	"github.com/kafka-csv-dump/config"
	"github.com/kafka-csv-dump/consumer"
)

func main() {
	if !config.Parse() {
		return
	}
	consumer.Init()
}
