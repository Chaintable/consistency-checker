package check

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"log"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/segmentio/kafka-go"

	"github.com/Chaintable/consistency_checker/config"
	"github.com/Chaintable/consistency_checker/db"
	"github.com/Chaintable/consistency_checker/nodes"
	"github.com/Chaintable/pipeline/types"
	"github.com/Chaintable/pipeline/util"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type Checker struct {
	innerNewBlockReader           *kafka.Reader
	innerReplicaStateChangeWriter *kafka.Writer
	outerNewBlockWriter           *kafka.Writer
	innerS3Reader                 *s3manager.Downloader
	confg                         *config.Config
	LatestBlockNumber             uint64
	quit                          chan struct{}
}

func NewKafkaReader(brokers []string, topic string, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})
}

func NewKafkaWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		BatchSize:    1,
	}
}

func NewS3Reader(region string) (*s3manager.Downloader, error) {
	s3Session, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return nil, err
	}
	return s3manager.NewDownloader(s3Session), nil
}

func NewChecker(config *config.Config) *Checker {
	err := db.OpenConsistencyDB(config.ConsistencyDBPath)
	if err != nil {
		log.Fatalf("open db error %+v", err)
	}

	innerS3Reader, err := NewS3Reader(config.InnerS3Region)
	if err != nil {
		log.Fatalf("create s3 reader error %+v", err)
	}

	return &Checker{
		innerNewBlockReader:           NewKafkaReader(config.InnerBrokers, config.InnerReplicaStateChangeTopic, config.InnerNewBlockGroupID),
		innerReplicaStateChangeWriter: NewKafkaWriter(config.InnerBrokers, config.InnerReplicaStateChangeTopic),
		innerS3Reader:                 innerS3Reader,
		outerNewBlockWriter:           NewKafkaWriter(config.OuterBrokers, config.OuterNewBlockTopic),
		confg:                         config,
		quit:                          make(chan struct{}),
	}
}

func (c *Checker) Close() {
	close(c.quit)
}

func (c *Checker) check(kafkaLatestBlockNumber uint64) (*types.ReplicaStateChangeNotification, error) {
	nodeStates := nodes.NodeMap.CheckAll(kafkaLatestBlockNumber)
	readyNodes := 0
	for _, nodeState := range nodeStates {
		if nodeState.StateType == 1 {
			readyNodes++
		}
	}
	if float64(readyNodes)/float64(len(nodeStates)) >= c.confg.ReadyRatio {
		for _, nodeState := range nodeStates {
			if nodeState.StateType == 1 {
				if c.LatestBlockNumber < nodeState.LatestBlockNumber.ToInt().Uint64() {
					c.LatestBlockNumber = nodeState.LatestBlockNumber.ToInt().Uint64()
				}
			}
		}
		return &types.ReplicaStateChangeNotification{
			LatestBlockNumber: (*hexutil.Big)(big.NewInt(int64(c.LatestBlockNumber))),
		}, nil
	}
	return nil, nil
}

func (c *Checker) checkWithReTry(knownedlatestBlockNumber uint64) (*types.ReplicaStateChangeNotification, error) {
	if c.LatestBlockNumber > knownedlatestBlockNumber {
		return nil, nil
	}
	for i := 0; i < 3; i++ {
		replicaStateChange, err := c.check(knownedlatestBlockNumber)
		if err != nil {
			return nil, err
		}
		if replicaStateChange != nil {
			return replicaStateChange, nil
		}
		time.Sleep(time.Duration(c.confg.CheckInterval) * time.Millisecond)
	}
	return nil, fmt.Errorf("check many times but not ready")
}

func (c *Checker) Process(blockNotice *types.BlockChangeNotification) bool {
	latestBlockNumber := blockNotice.NewBlocks[len(blockNotice.NewBlocks)-1].BlockNumber
	replicaStateChange, err := c.checkWithReTry(latestBlockNumber)
	if err != nil {
		log.Printf("check error %+v", err)
		return false
	}
	if replicaStateChange != nil {
		err = WriteReplicaStateChange(c.innerReplicaStateChangeWriter, replicaStateChange)
		if err != nil {
			return false
		}
	}

	err = WriteBlockNotice(c.outerNewBlockWriter, blockNotice)
	return err == nil
}

func (c *Checker) Run() {
	for {
		select {
		case <-c.quit:
			goto shutdown
		default:
			msg, err := c.innerNewBlockReader.FetchMessage(context.Background())
			if err != nil {
				log.Printf("fetch message error %+v", err)
				time.Sleep(1 * time.Second)
				continue
			}
			blockNotice := &types.BlockChangeNotification{}
			err = util.DecodeFromGzipJson(msg.Value, blockNotice)
			if err != nil {
				log.Printf("decode message error %+v", err)
				time.Sleep(1 * time.Second)
				continue
			}
			for {
				if !c.Process(blockNotice) {
					log.Printf("process error %+v", err)
					time.Sleep(1 * time.Second)
					continue
				} else {
					break
				}
			}
			for {
				err = c.innerNewBlockReader.CommitMessages(context.Background(), msg)
				if err != nil {
					log.Printf("CommitMessages message error %+v", err)
					time.Sleep(1 * time.Second)
					continue
				} else {
					break
				}
			}
		}
	}

shutdown:
	c.innerNewBlockReader.Close()
	c.outerNewBlockWriter.Close()
}

func WriteReplicaStateChange(writer *kafka.Writer, replicaStateChange *types.ReplicaStateChangeNotification) error {
	value, err := util.EncodeToJsonGzip(replicaStateChange)
	if err != nil {
		return err
	}
	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("ReplicaStateChange"),
		Value: value,
	})
	if err != nil {
		return err
	}
	return nil
}

func WriteBlockNotice(writer *kafka.Writer, blockNotice *types.BlockChangeNotification) error {
	value, err := util.EncodeToJsonGzip(blockNotice)
	if err != nil {
		return err
	}
	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("NewBlock"),
		Value: value,
	})
	if err != nil {
		return err
	}
	return nil
}
