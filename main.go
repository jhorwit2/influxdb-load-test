package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/influxdb/influxdb/client"
	"github.com/rcrowley/go-metrics"
)

type configuration struct {
	host            *string
	port            *int
	db              *string
	measurement     *string
	retentionPolicy *string
	batchSize       *int
	rate            *int
	cpus            *int
	duration        *int
}

func main() {

	config := configuration{
		host:            flag.String("h", "localhost", "host"),
		port:            flag.Int("p", 8086, "port number"),
		db:              flag.String("db", "load_test", "database"),
		measurement:     flag.String("m", "load_test", "measurement"),
		retentionPolicy: flag.String("rp", "default", "retention policy"),
		batchSize:       flag.Int("batchSize", 5000, "batch size for requests"),
		rate:            flag.Int("rate", 5, "requests per second"),
		cpus:            flag.Int("cpus", runtime.NumCPU(), "Number of CPUs to use"),
		duration:        flag.Int("duration", 5, "time in seconds for test to run"),
	}

	flag.Parse()

	runtime.GOMAXPROCS(*config.cpus)

	u, _ := url.Parse(fmt.Sprintf("http://%s:%d", *config.host, *config.port))
	con, err := client.NewClient(client.Config{URL: *u})

	if err != nil {
		panic(err)
	}

	// Log metrics every second
	go metrics.Log(metrics.DefaultRegistry, time.Second, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

	startLoadTest(con, config)
}

func startLoadTest(con *client.Client, config configuration) {
	durationCounter := 0

	t := metrics.NewTimer()
	metrics.Register("bang", t)

	for _ = range time.Tick(time.Second) {
		if durationCounter >= *config.duration {
			return
		}

		for i := 0; i < *config.rate; i++ {
			go func() {
				t.Time(func() {
					writePoints(con, config)
				})
			}()
		}
		durationCounter++
	}
}

func writePoints(con *client.Client, config configuration) {
	var (
		hosts     = []string{"host1", "host2", "host3", "host4", "host5", "host6"}
		metrics   = []string{"com.addthis.Service.total._red_pjson__.1MinuteRate", "com.addthis.Service.total._red_lojson_100eng.json.1MinuteRate", "com.addthis.Service.total._red_lojson_300lo.json.1MinuteRate"}
		batchSize = *config.batchSize
		pts       = make([]client.Point, batchSize)
	)

	rand.Seed(42)
	for i := 0; i < batchSize; i++ {
		pts[i] = client.Point{
			Measurement: *config.measurement,
			Tags: map[string]string{
				"host":   strconv.Itoa(rand.Intn(len(hosts))),
				"metric": strconv.Itoa(rand.Intn(len(metrics))),
			},
			Fields: map[string]interface{}{
				"value": rand.Float64(),
			},
			Precision: "n",
		}
	}

	bps := client.BatchPoints{
		Points:          pts,
		Database:        *config.db,
		RetentionPolicy: *config.retentionPolicy,
	}
	_, err := con.Write(bps)
	if err != nil {
		log.Fatal(err)
	}
}
