package consumer

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/kafka-csv-dump/config"
	"github.com/kataras/golog"
)

func Init() {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Props.Kafka.Brokers, ","),
		"group.id":          "testcg",
	})
	if err != nil {
		golog.Error(err)
		return
	}
	// c.Subscribe("test1", nil)
	topic := "test1"
	c.Assign([]kafka.TopicPartition{{
		Topic:     &topic,
		Partition: 1,
		Offset:    -1,
	}})
	if err != nil {
		golog.Error(err)
		return
	}

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			golog.Infof("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// The client will automatically try to recover from all errors.
			golog.Errorf("Consumer error: %v (%v)\n", err, msg)
		}
	}
	c.Close()
}
