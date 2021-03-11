package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/kataras/golog"
)

type Config struct {
	Env   string
	Kafka Kafka
}
type Kafka struct {
	Brokers  []string
	Consumer Consumer
	Topics   []Topic
}

type Consumer struct {
}
type Topic struct {
	Name     string
	Partions [2]int
}

var Props Config
var confPath string
var fileSep string

func Parse() bool {
	confPath = os.Getenv("CONFIG")
	fileSep = os.Getenv("FILE_SEP")
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

	return nil
}
