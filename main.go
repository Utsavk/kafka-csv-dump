package main

import (
	"github.com/kafka-csv-dump/config"
)

func main() {
	if !config.Parse() {
		return
	}
}
