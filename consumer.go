package main

import (
	"encoding/json"
	"log"

	"github.com/bitly/go-nsq"
)

type Consumer struct {
	consumer *nsq.Consumer
	channel  chan *nsq.Message

	Topic     string
	URI       string
	Publisher IPublisher
}

func (c *Consumer) Start() error {
	var err error
	config := nsq.NewConfig()
	if c.consumer, err = nsq.NewConsumer(c.Topic, "ch", config); err != nil {
		return err
	}
	c.channel = make(chan *nsq.Message)

	c.consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		obj, err := convertMessage(message)
		if err != nil {
			log.Println(err)
			return err
		}
		if err := c.Publisher.Publish(obj); err != nil {
			log.Println(err)
			return err
		}
		return nil
	}))

	if err := c.consumer.ConnectToNSQD(c.URI); err != nil {
		return err
	}
	return nil
}

func (c *Consumer) Stop() error {
	c.consumer.Stop()
	return nil
}

func convertMessage(msg *nsq.Message) (*ipfix, error) {
	objmap := ipfix{}
	err := json.Unmarshal(msg.Body, &objmap)
	return &objmap, err
}
