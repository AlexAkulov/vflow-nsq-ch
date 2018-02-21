package main

import (
	"syscall"
	"os/signal"
	"os"
	"flag"
	"log"
)

type options struct {
	NSQDUri string
	Topic   string
	Debug   bool
	Workers int
	CHUri string
	CHTable string
	BatchSize int
	BatchPeriod int
	DryRun bool
}

type dataField struct {
	I int
	V interface{}
}

type ipfix struct {
	AgentID  string
	DataSets [][]dataField
}

type dIPFIXSample struct {
	device string
	src    string
	dst    string
	srcASN uint64
	dstASN uint64
	proto  uint8
}

var opts options

func init() {
	flag.StringVar(&opts.NSQDUri, "nsqd", "127.0.0.1:4150", "nsqd ipaddress:port")
	flag.StringVar(&opts.Topic, "topic", "nsq", "nsq topic")
	flag.StringVar(&opts.CHUri, "ch-uri", "http://127.0.0.1:9000?debug=false", "ClickHouse URI proto://ipaddress:port")
	flag.StringVar(&opts.CHTable, "ch-table", "wssg.vflow", "ClickHouse table")
	flag.IntVar(&opts.BatchSize, "batch-size", 1000, "Max rows in batch to ClickHouse")
	flag.IntVar(&opts.BatchPeriod, "batch-period", 10, "Max period between batches to ClickHouse in seconds")
	flag.BoolVar(&opts.DryRun, "dry-run", false, "Dry Run")

	flag.Parse()
}

func main() {

	p := &Publisher{
		BatchSize: uint(opts.BatchSize),
		BatchPeriod: int64(opts.BatchPeriod),
		URI: opts.CHUri,
		Table: opts.CHTable,
		DryRun: opts.DryRun,
	}

	if err := p.Start(); err != nil {
		log.Fatal(err)
	}

	c := &Consumer{
		Topic: opts.Topic,
		URI: opts.NSQDUri,
		Publisher: p,
	}

	if err := c.Start(); err != nil {
		log.Fatal(err)
	}
	log.Println("Started")

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	log.Println(<-signalChannel)

	if err := c.Stop(); err != nil {
		log.Print("Can't stop Consumer with err: ", err)
	}

	if err := p.Stop(); err != nil {
		log.Print("Can't stop Publisher with err: ", err)
	}
	log.Println("Stopped")
}




