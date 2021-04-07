package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/kataras/golog"
)

type Config struct {
	Env      string
	Kafka    Kafka
	Consumer Consumer
}

type Kafka struct {
	Brokers []string
	Topics  []Topic
}

type Consumer struct {
	Group             string
	ReadTimeOutMS     int64
	StartOffset       int64
	PollingIntervalMS int64
}

type Topic struct {
	Name           string
	Partitions     [2]int
	StartOffset    int64
	Schema         string
	FilteredFields []string
}

var Props Config
var confPath string

func Parse() bool {
	confPath = os.Getenv("CONFIG")
	if confPath == "" {
		golog.Info("no config path provided, parsing conf from <PROJECT_DIR/>service.json")
		confPath = "service.json"
	}
	confData, err := ioutil.ReadFile(confPath)
	if err != nil {
		golog.Error(err)
		return false
	}
	err = json.Unmarshal(confData, &Props)
	if err != nil {
		golog.Error(err)
		return false
	}
	err = validate()
	if err != nil {
		golog.Error(err)
		return false
	}
	return true
}

func validate() error {
	if len(Props.Kafka.Brokers) == 0 {
		return fmt.Errorf("missing kafka brokers list")
	}
	err := validateConsumerConf()
	return err
}

func validateConsumerConf() error {
	//FilteredFields and Schema are mandatory
	return nil
}
