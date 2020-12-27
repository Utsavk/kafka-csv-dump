package main

import (
	"github.com/kafka-csv-dump/config"
	"github.com/kataras/golog"
)

func main() {
	if !config.Parse() {
		return
	}
	golog.Info(config.Props)
}
