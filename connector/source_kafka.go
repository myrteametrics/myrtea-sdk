package connector

import (
	"os"

	cluster "github.com/bsm/sarama-cluster"
	"go.uber.org/zap"
)

type KafkaSource struct {
	Topic     string
	Consumer  *cluster.Consumer
	OutChanel chan Message
	Signals   chan os.Signal
}

func NewKafkaSource(topic string, consumer *cluster.Consumer, out chan Message, signals chan os.Signal) *KafkaSource {
	source := KafkaSource{
		Topic:     topic,
		Consumer:  consumer,
		Signals:   signals,
		OutChanel: out,
	}
	return &source
}

func (src *KafkaSource) Init() error {
	return nil
}

func (src *KafkaSource) Close() error {
	return nil
}

func (src *KafkaSource) Run() error {
	zap.L().Sugar().Infof("Starting consumer on topic=%s", src.Topic)

	go func() {
		for err := range src.Consumer.Errors() {
			zap.L().Error("Error:", zap.Error(err))
		}
	}()

	go func() {
		for ntf := range src.Consumer.Notifications() {
			zap.L().Sugar().Infof("Rebalanced: %+v\n", ntf)
		}
	}()

	for {
		select {
		case msg, ok := <-src.Consumer.Messages():
			if ok {
				newMsg := KafkaMessage{
					Data: msg.Value,
				}
				src.OutChanel <- newMsg
				src.Consumer.MarkOffset(msg, "")
			}
		}
	}
}
