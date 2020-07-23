package go_metrics_clickhouse

import (
	"fmt"
	"log"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
	"github.com/rcrowley/go-metrics"
)

type reporter struct {
	reg      metrics.Registry
	interval time.Duration
	align    bool // align to interval

	hostport string // ClickHouse host & port. port should be TCP port (not http api port) ex) host1:9000
	database string // ClickHouse database
	table    string // ClickHouse table

	username string
	password string
	tags     []string

	chconn *sqlx.DB // ClickHouse connection
}

// ClickHouse starts a ClickHouse reporter which will post the metrics from the given registry at each d interval.
func ClickHouse(r metrics.Registry, d time.Duration, hostport, database, table, username, password string, align bool) {
	ClickHouseWithTags(r, d, hostport, database, table, username, password, map[string]string{}, align)
}

// ClickHouseWithTags starts a ClickHouse reporter which will post the metrics from the given registry at each d interval with the specified tags
func ClickHouseWithTags(r metrics.Registry, d time.Duration, hostport, database, table, username, password string, tags map[string]string, align bool) {

	rep := &reporter{
		reg:      r,
		interval: d,
		hostport: hostport,
		database: database,
		table:    table,
		username: username,
		password: password,
		tags:     makeTags(tags),
		align:    align,
	}
	if err := rep.makeClient(); err != nil {
		log.Printf("unable to make ClickHouse client. err=%v", err)
		return
	}

	rep.run()
}

func (r *reporter) makeClient() (err error) {
	// create user info part of connection string
	userstring := ""
	if len(r.username) > 0 && len(r.password) > 0 {
		userstring = fmt.Sprintf("username=%s&password=%s&", r.username, r.password)
	}
	// assemble connection string
	//connstring := fmt.Sprintf("tcp://%s?%sdatabase=%s", r.hostport, userstring, r.database)
	connstring := fmt.Sprintf("tcp://%s?%s", r.hostport, userstring)

	// open sqlx database connection
	r.chconn, err = sqlx.Open("clickhouse", connstring)

	// create database & table on clickhouse to store metrics if not exists
	cdstring := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, r.database)
	_, errCreateDatabase := r.chconn.Exec(cdstring)
	if errCreateDatabase != nil {
		log.Fatalf("Error creating clickhouse database: %v", errCreateDatabase)
	}

	ctstring := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s
		(
			time DateTime CODEC(DoubleDelta, LZ4),
			name String CODEC(LZ4),
			tags Array(String) CODEC(LZ4),
			value Float32
		) ENGINE = MergeTree()
		  ORDER BY (time, name)
	`, r.database, r.table)
	_, errCreateTable := r.chconn.Exec(ctstring)

	if errCreateTable != nil {
		log.Fatalf("Error creating clickhouse table: %v", errCreateTable)
	}

	return
}

type metricRow struct {
	mtime time.Time
	name  string
	tags  []string
	fval  float32
}

func (r *reporter) run() {
	intervalTicker := time.Tick(r.interval)
	pingTicker := time.Tick(time.Second * 5)

	for {
		select {
		case <-intervalTicker:
			if err := r.send(); err != nil {
				log.Printf("unable to send metrics to ClickHouse. err=%v", err)
			}
		case <-pingTicker:
			err := r.chconn.Ping()
			if err != nil {
				log.Printf("got error while sending a ping to ClickHouse, trying to recreate client. err=%v", err)

				if err = r.makeClient(); err != nil {
					log.Printf("unable to make ClickHouse client. err=%v", err)
				}
			}
		}
	}
}

func (r *reporter) send() error {
	//var pts []client.Point
	var pts []metricRow

	now := time.Now()
	if r.align {
		now = now.Truncate(r.interval)
	}
	r.reg.Each(func(name string, i interface{}) {

		switch metric := i.(type) {
		case metrics.Counter:
			ms := metric.Snapshot()
			pts = append(pts, metricRow{
				mtime: now,
				name:  fmt.Sprintf("%s.count", name),
				tags:  r.tags,
				fval:  float32(ms.Count()),
			})
		case metrics.Gauge:
			ms := metric.Snapshot()
			pts = append(pts, metricRow{
				mtime: now,
				name:  fmt.Sprintf("%s.gauge", name),
				tags:  r.tags,
				fval:  float32(ms.Value()),
			})
		case metrics.GaugeFloat64:
			ms := metric.Snapshot()
			pts = append(pts, metricRow{
				mtime: now,
				name:  fmt.Sprintf("%s.gauge", name),
				tags:  r.tags,
				fval:  float32(ms.Value()),
			})
		case metrics.Histogram:
			ms := metric.Snapshot()
			ps := ms.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			fields := map[string]float64{
				"count":    float64(ms.Count()),
				"max":      float64(ms.Max()),
				"mean":     ms.Mean(),
				"min":      float64(ms.Min()),
				"stddev":   ms.StdDev(),
				"variance": ms.Variance(),
				"p50":      ps[0],
				"p75":      ps[1],
				"p95":      ps[2],
				"p99":      ps[3],
				"p999":     ps[4],
				"p9999":    ps[5],
			}
			for k, v := range fields {
				pts = append(pts, metricRow{
					mtime: now,
					name:  fmt.Sprintf("%s.histogram", name),
					tags:  makeBucketTags(k, r.tags),
					fval:  float32(v),
				})
			}
		case metrics.Meter:
			ms := metric.Snapshot()
			fields := map[string]float64{
				"count": float64(ms.Count()),
				"m1":    ms.Rate1(),
				"m5":    ms.Rate5(),
				"m15":   ms.Rate15(),
				"mean":  ms.RateMean(),
			}
			for k, v := range fields {
				pts = append(pts, metricRow{
					mtime: now,
					name:  fmt.Sprintf("%s.meter", name),
					tags:  makeBucketTags(k, r.tags),
					fval:  float32(v),
				})
			}
		case metrics.Timer:
			ms := metric.Snapshot()
			ps := ms.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			fields := map[string]float64{
				"count":    float64(ms.Count()),
				"max":      float64(ms.Max()),
				"mean":     ms.Mean(),
				"min":      float64(ms.Min()),
				"stddev":   ms.StdDev(),
				"variance": ms.Variance(),
				"p50":      ps[0],
				"p75":      ps[1],
				"p95":      ps[2],
				"p99":      ps[3],
				"p999":     ps[4],
				"p9999":    ps[5],
				"m1":       ms.Rate1(),
				"m5":       ms.Rate5(),
				"m15":      ms.Rate15(),
				"meanrate": ms.RateMean(),
			}
			for k, v := range fields {
				pts = append(pts, metricRow{
					mtime: now,
					name:  fmt.Sprintf("%s.timer", name),
					tags:  makeBucketTags(k, r.tags),
					fval:  float32(v),
				})
			}
		}
	})

	// begin transaction
	tx, _ := r.chconn.Begin()

	// prepare insert statement
	insertString := fmt.Sprintf(`INSERT INTO %s.%s (time, name, tags, value) VALUES (?, ?, ?, ?)`, r.database, r.table)
	insertStmt, errPrepare := tx.Prepare(insertString)

	if errPrepare != nil {
		log.Fatalf("Error preparing insert statement: %v", errPrepare)
	}
	defer insertStmt.Close()

	// insert loop
	for _, pt := range pts {
		// execute insert for each metric points
		if _, errInsertStmt := insertStmt.Exec(pt.mtime, pt.name, pt.tags, pt.fval); errInsertStmt != nil {
			log.Fatalf("Error insert to ClickHouse: %v", errInsertStmt)
		}
	}

	// commit transaction
	if errCommit := tx.Commit(); errCommit != nil {
		log.Fatalf("Error commit transaction: %v", errCommit)
	}

	return nil
}

func makeTags(tags map[string]string) []string {
	rval := []string{}
	for tk, tv := range tags {
		kvstr := fmt.Sprintf("%s=%s", tk, tv)
		rval = append(rval, kvstr)
	}
	return rval
}

func makeBucketTags(bucket string, tags []string) []string {
	rval := append(tags, fmt.Sprintf("bucket=%s", bucket))
	return rval
}
