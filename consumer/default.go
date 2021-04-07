package consumer

import (
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/kafka-csv-dump/config"
	"github.com/kafka-csv-dump/core"
	"github.com/kataras/golog"
)

var readTimoutDur, pollingInterval time.Duration

type consumer struct {
	kobj      *kafka.Consumer
	Offset    int64
	Topic     string
	Partition int
}

func Init() {
	readTimoutDur = time.Duration(config.Props.Consumer.ReadTimeOutMS) * time.Millisecond
	pollingInterval = time.Duration(config.Props.Consumer.PollingIntervalMS) * time.Millisecond
	launchConsumers()
}

func launchConsumers() {
	var wg sync.WaitGroup
	for _, topicConf := range config.Props.Kafka.Topics {
		for _, partition := range topicConf.Partitions {
			wg.Add(1)
			go func(partition int, topicConf config.Topic) {
				defer func() {
					wg.Done()
					if r := recover(); r != nil {
						golog.Infof("Recovering from panic in consumer init for topic %s and partition %d, error is: %v \n", topicConf.Name, partition, r)
					}
				}()
				startOffset := getStartOffsetValue(topicConf, partition)
				c, err := createConsumerInstance(topicConf.Name, partition, startOffset)
				if err != nil {
					golog.Errorf("error in creating consumer instance %s", err.Error())
					return
				}
				defer c.kobj.Close()
				golog.Infof("Consumer started, topic=%s partition=%d offset=%d", topicConf.Name, partition, startOffset)
				c.pollTopicPartiton(partition, topicConf)
			}(partition, topicConf)
		}
	}
	wg.Wait()
}

func (c *consumer) pollTopicPartiton(partition int, topicConf config.Topic) {
	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					golog.Infof("Recovering from panic in pollTopicPartiton error is: %v \n", r)
				}
			}()
			msg, err := c.kobj.ReadMessage(readTimoutDur)
			if err != nil {
				if !strings.Contains(err.Error(), "Local: Timed out") {
					golog.Errorf("Consumer error: %v (%v)\n", err, msg)
				}
				return
			}
			if msg == nil {
				golog.Errorf("Nil Message on %s %s", "test1", msg.TopicPartition)
			} else {
				core.ProcessData(partition, topicConf, msg.Value)
			}
		}()
		time.Sleep(pollingInterval)
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
	c.Assign([]kafka.TopicPartition{{
		Topic:     &topic,
		Partition: int32(partition),
		Offset:    kafka.Offset(startOffset),
	}})
	if err != nil {
		return nil, err
	}
	cons := &consumer{kobj: c, Offset: startOffset, Topic: topic, Partition: partition}
	return cons, nil
}
