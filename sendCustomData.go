package main

import (
	"encoding/json"
	"errors"
	"log"
	"regexp"
	"strings"

	"fmt"

	"github.com/Shopify/sarama"
)

func sendCustomData(data []*MetaData) error {
	if cfg.KafkaBrokers == "" {
		return nil
	}
	for _, d := range data {
		switch d.Metric {
		case "CustomData_Process_List":
			portReg := regexp.MustCompile(`.*(port=([0-9]+)).*`)
			port := portReg.FindAllString(d.Tags, 1)[0]
			p := &ProcessList{
				Endpoint:  d.Endpoint + ":" + port[5:],
				Value:     d.Value,
				Timestamp: d.Timestamp,
			}
			js, err := json.Marshal(p)
			if err != nil {
				return err
			}
			partition, offset, err := kafkaProducer().SendMessage(&sarama.ProducerMessage{
				Topic: cfg.KafkaTopic,
				Value: sarama.ByteEncoder(js),
			})
			fmt.Println(partition, offset, err)
		default:
			return errors.New("不支持")
		}
	}
	return nil
}

func kafkaProducer() sarama.SyncProducer {
	config := sarama.NewConfig()
	fmt.Println(config)
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true

	// On the broker side, you may want to change the following settings to get
	// stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.
	brokerslist := strings.Split(cfg.KafkaBrokers, ",")
	fmt.Println(brokerslist)
	producer, err := sarama.NewSyncProducer(brokerslist, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}
	return producer
}
