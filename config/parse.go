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
	Brokers        []string
	TopicsFilePath string
	Consumer       Consumer
	Topics         map[string]Topic
}

type Consumer struct {
}
type Topic struct {
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
	if Props.Kafka.TopicsFilePath == "" {
		confBase, _ := utils.GetFileBaseDir(confPath, fileSep)
		if confBase == "" {
			Props.Kafka.TopicsFilePath = "topics.json"
		} else {
			Props.Kafka.TopicsFilePath = confBase + "/topics.json"
		}
	}
	topicsData, err := ioutil.ReadFile(Props.Kafka.TopicsFilePath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(topicsData, &Props.Kafka.Topics)
	if err != nil {
		return err
	}
	return nil
}
