package consumer

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/kafka-csv-dump/config"
	"github.com/kataras/golog"
)

type consumer struct {
	kobj   *kafka.Consumer
	Offset int64
	Topic  string
}

func Init() {
	c, err := createConsumerInstance("test1", 1, -1)
	if err != nil {
		golog.Errorf("error in creating consumer instance %s", err.Error())
		return
	}
	c.pollTopicPartiton()
}

func (c *consumer) pollTopicPartiton() {
	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					golog.Infof("Recovering from panic in pollTopicPartiton error is: %v \n", r)
				}
			}()
			msg, err := c.kobj.ReadMessage(-1)
			if err != nil {
				golog.Errorf("Consumer error: %v (%v)\n", err, msg)
				return
			}
			if msg == nil {
				golog.Errorf("Nil Message on %s %s", "test1", msg.TopicPartition)
			} else {
				golog.Infof("Message on %s %s: %s\n", "test1", msg.TopicPartition, string(msg.Value))
			}
		}()
	}
}

func createConsumerInstance(topic string, partition int, startOffset int64) (*consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Props.Kafka.Brokers, ","),
		"group.id":          config.Props.Consumer.Group,
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		c.Close()
	}()
	c.Assign([]kafka.TopicPartition{{
		Topic:     &topic,
		Partition: int32(partition),
		Offset:    kafka.Offset(startOffset),
	}})
	if err != nil {
		return nil, err
	}
	cons := &consumer{kobj: c, Offset: startOffset, Topic: topic}
	return cons, nil
}
