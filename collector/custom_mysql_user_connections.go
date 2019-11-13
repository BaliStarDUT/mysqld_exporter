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

const infoSchemaProcesslist_custom_UserQuery2 = `
		  SELECT
		    user,
            count(*) as num
          FROM information_schema.processlist
          WHERE user not in ('root','system user','replic')
          GROUP BY user order by num desc
		`
const infoSchemaProcesslist_custom_UserQuery = `
		  SELECT
		    user,
   		    SUBSTRING_INDEX(host, ':', 1) AS host,
		    COALESCE(command,'') AS command,
		    COALESCE(state,'') AS state,
		    count(*) AS processes,
		    sum(time) AS seconds
          FROM information_schema.processlist
		  WHERE user not in ('root','system user','replic')
          GROUP BY user order by processes desc
		`
// Tunable flags.
var (
	processesBy_custom_UserFlag = kingpin.Flag(
		"collect.info_schema.processlist.custom_processes_by_user",
		"Enable collecting the number of processes by custom user",
	).Default("true").Bool()
)

// Metric descriptors.
var (
	processesBy_custom_UserDesc = prometheus.NewDesc(
		prometheus.BuildFQName(custom_space, informationSchema, "user_connections"),
		"The number of processes by custom user.",
		[]string{"db_user"}, nil)
)

// ScrapeUserConnections collects from `information_schema.processlist`.
type ScrapeUserConnections struct{}

// Name of the Scraper. Should be unique.
func (ScrapeUserConnections) Name() string {
	return "custom."+informationSchema + ".processlist"
}

// Help describes the role of the Scraper.
func (ScrapeUserConnections) Help() string {
	return "Collect custom data from information_schema.processlist"
}

// Version of MySQL from which scraper is available.
func (ScrapeUserConnections) Version() float64 {
	return 5.1
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeUserConnections) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
    processQuery := fmt.Sprintf(infoSchemaProcesslist_custom_UserQuery)
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
    userCount := make(map[string]uint32)

    for processlistRows.Next() {
        err = processlistRows.Scan(&user, &host, &command, &state, &processes, &time)
        if err != nil {
            return err
        }
        userCount[user] = userCount[user] + processes
    }
    if *processesBy_custom_UserFlag {
        for user, processes := range userCount {
            ch <- prometheus.MustNewConstMetric(processesBy_custom_UserDesc, prometheus.GaugeValue, float64(processes), user)
        }
    }
	level.Info(logger).Log("msg", "custom user connections collect end.")
    return nil
}

// check interface
var _ Scraper = ScrapeUserConnections{}
