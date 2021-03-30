package consumer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/kafka-csv-dump/config"
)

func getStartOffsetValue(topic config.Topic, partition int) int64 {
	if topic.StartOffset != 0 {
		return topic.StartOffset
	}
	if config.Props.Consumer.StartOffset != 0 {
		return config.Props.Consumer.StartOffset
	}
	return int64(kafka.OffsetBeginning)
}
