package main

import (
	"fmt"
	"log"
	"time"
	"database/sql"

	"github.com/facebookgo/muster"
	_ "github.com/mailru/go-clickhouse"
)

type Publisher struct {
	connect *sql.DB
	muster  *muster.Client

	BatchSize   uint
	BatchPeriod int64
	URI         string
	Table       string
}

type batch struct {
	Publisher *Publisher
	Items     []*ipfix
}

func (p *Publisher) Start() error {
	var err error
	if p.connect, err = sql.Open("clickhouse", p.URI); err != nil {
		return err
	}

	if err := p.connect.Ping(); err != nil {
		return err
	}

	p.muster = &muster.Client{
		MaxBatchSize:         p.BatchSize,
		MaxConcurrentBatches: 1,
		BatchTimeout:         time.Duration(p.BatchPeriod) * time.Second,
		BatchMaker:           p.batchMaker,
	}
	return p.muster.Start()
}

func (p *Publisher) Publish(obj *ipfix) error {
	p.muster.Work <- obj
	return nil
}

func (p *Publisher) batchMaker() muster.Batch {
	return &batch{
		Publisher: p,
	}
}

func (b *batch) Add(item interface{}) {
	b.Items = append(b.Items, item.(*ipfix))
}

func (b *batch) Fire(notifier muster.Notifier) {
	defer notifier.Done()
	tx, err := b.Publisher.connect.Begin()
	if err != nil {
		log.Printf("Can't begin connection to ClickHouse with err: '%v' Lost: %d messages", err, len(b.Items))
		return
	}
	stmt, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s (Date,TimeStamp,Device,Src,Dst,SrcASN,DstASN,Proto) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", b.Publisher.Table))
	if err != nil {
		log.Printf("Can't prepare connection to Clickhouse with err: '%v' Lost: %d messages", err, len(b.Items))
	}
	for _, sample := range b.Items {
		for _, data := range sample.DataSets {
			s := dIPFIXSample{}
			for _, dd := range data {
				switch dd.I {
				case 8, 27:
					s.src = dd.V.(string)
				case 12, 28:
					s.dst = dd.V.(string)
				case 16:
					s.srcASN = uint64(dd.V.(float64))
				case 17:
					s.dstASN = uint64(dd.V.(float64))
				case 4:
					s.proto = uint8(dd.V.(float64))
				}
			}
			if _, err := stmt.Exec(time.Now(), time.Now(), sample.AgentID, s.src, s.dst, s.srcASN, s.dstASN, s.proto); err != nil {
				log.Printf("Can't append Dataset with err: '%v' Skipping", err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		log.Printf("Can't commit batch to ClickHouse wuth err: '%v' Lost: %d messages", err, len(b.Items))
	}
}

func (p *Publisher) Stop() error {
	return p.muster.Stop()
}
