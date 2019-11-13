package collector

import (
	"context"
    "github.com/go-kit/kit/log/level"
	"database/sql"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

const infoSchemaProcesslist_custom_SlowQuery = `
			SELECT
			  user,
			  SUBSTRING_INDEX(host, ':', 1) AS host,
			  COALESCE(command,'') AS command,
			  COALESCE(state,'') AS state,
			  count(*) AS processes,
			  sum(time) AS seconds
			FROM information_schema.processlist
			WHERE ID != connection_id()
			  AND TIME >= %d
			GROUP BY user,SUBSTRING_INDEX(host, ':', 1),command,state
			ORDER BY null
		`
// Tunable flags.
var (
	processlist_custom_MinTime = kingpin.Flag(
		"collect.info_schema.processlist.custom_min_time",
		"Slow queries run in min time to be counted",
	).Default("60").Int()
	processesBy_custom_SlowQueriesFlag = kingpin.Flag(
		"collect.info_schema.processlist.custom_slow_queries_60",
		"Enable collecting the the slow queries",
	).Default("true").Bool()
)

// Metric descriptors.
var (
	processlist_custom_TimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(custom_space, informationSchema, "slow_queries_60"),
		"The number of seconds threads (connections) have used split by current state.",
		[]string{"state"}, nil)

)

// ScrapeSlowQueries collects from `information_schema.processlist`.
type ScrapeSlowQueries struct{}

// Name of the Scraper. Should be unique.
func (ScrapeSlowQueries) Name() string {
	return "custom."+informationSchema + ".slow_query_60"
}

// Help describes the role of the Scraper.
func (ScrapeSlowQueries) Help() string {
	return "Collect slow_query_60 from information_schema.processlist"
}

// Version of MySQL from which scraper is available.
func (ScrapeSlowQueries) Version() float64 {
	return 5.1
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeSlowQueries) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
	processQuery := fmt.Sprintf(
		infoSchemaProcesslist_custom_SlowQuery,
		*processlist_custom_MinTime,
	)
    processlistRows, err := db.QueryContext(ctx, processQuery)
    if err != nil {
        return err
    }
    defer processlistRows.Close()

    var (
        user      string
        host      string
        command   string
        state     string
        processes uint32
        time      uint32
    )

	stateTime := make(map[string]uint32, len(threadStateCounterMap))
	for k, v := range threadStateCounterMap {
		stateTime[k] = v
	}

	for processlistRows.Next() {
		err = processlistRows.Scan(&user, &host, &command, &state, &processes, &time)
		if err != nil {
			return err
		}
		realState := deriveThreadState(command, state)
		stateTime[realState] += time
	}

	for state, time := range stateTime {
		ch <- prometheus.MustNewConstMetric(processlist_custom_TimeDesc, prometheus.GaugeValue, float64(time), state)
	}
	level.Info(logger).Log("msg", "custom slow queries_60s collect end.")
	return nil
}

// check interface
var _ Scraper = ScrapeSlowQueries{}
