package go_metrics_clickhouse

import (
	"github.com/rcrowley/go-metrics"
	"strconv"
	"testing"
	"time"
)

func runc(c metrics.Counter, g metrics.GaugeFloat64) {
	ticker := time.NewTicker(time.Second * 1)
	v := float64(0)
	for _ = range ticker.C {
		c.Inc(1)
		v += 0.12
		g.Update(v)
	}
}

func TestClickHouse(t *testing.T) {

	mr := metrics.NewRegistry()
	cntr := metrics.NewCounter()
	mr.GetOrRegister("simple-counter", cntr)
	gau := metrics.NewGaugeFloat64()
	mr.GetOrRegister("simple-gauge", gau)

	metricTags := map[string]string{
		"node_id":   strconv.Itoa(1234),
		"node_name": "node001",
		"job_id":    strconv.Itoa(99),
		"process":   "process_name",
	}

	go ClickHouse(mr, time.Second*3, "localhost:9000", "temp2", "gomet", "", "", false)
	go ClickHouseWithTags(mr, time.Second*1, "localhost:9000", "temp3", "gomet", "", "", metricTags, false)

	runc(cntr, gau)
}
