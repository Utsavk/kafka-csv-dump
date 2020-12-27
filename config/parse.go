package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/kafka-csv-dump/utils"
	"github.com/kataras/golog"
)

type Config struct {
	Env   string
	Kafka Kafka
}
type Kafka struct {
	Brokers      []string
	ConsumerFile string
	Consumer     Consumer
}

type Consumer struct {
	Topics []Topic
}
type Topic struct {
	Name     string
	Partions int
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
	if Props.Kafka.ConsumerFile == "" {
		confBase, _ := utils.GetFileBaseDir(confPath, fileSep)
		Props.Kafka.ConsumerFile = confBase + "/consumer.json"
	}
	consConfData, err := ioutil.ReadFile(Props.Kafka.ConsumerFile)
	if err != nil {
		return err
	}
	err = json.Unmarshal(consConfData, &Props.Kafka.Consumer)
	if err != nil {
		return err
	}
	return nil
}
