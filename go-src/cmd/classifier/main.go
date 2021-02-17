package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
)

type processMetric struct {
	timestamp int64
	host      string
	pid       int
	cpuUsage  float64
	user      string
	cmdLine   string
}

func main() {

	var srcInfluxURL string
	flag.StringVar(&srcInfluxURL, "srcurl", "http://localhost:8086", "URL for source InfluxDB API endpoint")

	var dstInfluxURL string
	flag.StringVar(&dstInfluxURL, "dsturl", "", "URL for destination InfluxDB API endpoint (will use srcurl if empty)")

	var srcInfluxDB string
	flag.StringVar(&srcInfluxDB, "srcdb", "telegraf", "Source database")

	var srcInfluxRP string
	flag.StringVar(&srcInfluxRP, "srcrp", "autogen", "Source retention policy")

	var srcInfluxMeasurement string
	flag.StringVar(&srcInfluxMeasurement, "srcm", "procstat", "Source measurement")

	var dstInfluxDB string
	flag.StringVar(&dstInfluxDB, "dstdb", "", "Destination database (will use srcdb if empty)")

	var dstInfluxRP string
	flag.StringVar(&dstInfluxRP, "dstrp", "", "Destination retention policy (will use srcrp if empty)")

	var dstInfluxMeasurement string
	flag.StringVar(&dstInfluxMeasurement, "dstm", "classified_procstat", "Destination measurement")

	var periodSeconds int
	flag.IntVar(&periodSeconds, "period", 60, "Period (in seconds) to repeat classification")

	var windowSeconds int
	flag.IntVar(&windowSeconds, "window", 60, "Window (in seconds)")

	flag.Parse()

	c, err := client.NewHTTPClient(client.HTTPConfig{Addr: srcInfluxURL})
	if err != nil {
		log.Fatal("Error creating InfluxDB Client: ", err.Error(), srcInfluxURL)
	}
	defer c.Close()

	if dstInfluxURL == "" {
		dstInfluxURL = srcInfluxURL
	}
	wc, err := client.NewHTTPClient(client.HTTPConfig{Addr: dstInfluxURL})
	if err != nil {
		log.Fatal("Error creating InfluxDB Client: ", err.Error(), dstInfluxURL)
	}
	defer wc.Close()

	if dstInfluxDB == "" {
		dstInfluxDB = srcInfluxDB
	}
	if dstInfluxRP == "" {
		dstInfluxRP = srcInfluxRP
	}

	query := client.NewQuery(fmt.Sprintf("SELECT \"pid\",\"cpu_usage\",\"user\",\"cmdline\"  FROM \"%s\" WHERE time > now()-%ds group by \"host\"", srcInfluxMeasurement, windowSeconds), srcInfluxDB, srcInfluxRP)
	for {
		if response, err := c.Query(query); err == nil && response.Error() == nil {
			// iterate over the per host series
			for _, f := range response.Results {
				for _, s := range f.Series {
					host := s.Tags["host"]
					processMetrics := make([]*processMetric, 0)
					for _, v := range s.Values {
						pm, err := toProcessMetric(host, v)
						if err != nil {
							log.Fatal(err)
						}
						if len(processMetrics) == 0 {
							processMetrics = append(processMetrics, pm)
						} else {
							last := processMetrics[len(processMetrics)-1]
							if last.timestamp == pm.timestamp {
								processMetrics = append(processMetrics, pm)
							} else {
								// change in timestamp
								classified := processRawHostMetricsWithSameTimestamp(processMetrics)
								writeClassified(wc, dstInfluxMeasurement, dstInfluxDB, dstInfluxRP, host, last.timestamp, classified)
								processMetrics = make([]*processMetric, 0)
								processMetrics = append(processMetrics, pm)
							}
						}
					}
					if len(processMetrics) > 0 {
						classified := processRawHostMetricsWithSameTimestamp(processMetrics)
						writeClassified(wc, dstInfluxMeasurement, dstInfluxDB, dstInfluxRP, host, processMetrics[0].timestamp, classified)
					}
				}
			}

		} else {
			log.Fatal(err, response.Error())
		}

		time.Sleep(time.Duration(periodSeconds) * time.Second)
	}

}

func writeClassified(c client.Client, measurement, db, rp string, host string, timestamp int64, classifications map[string]*accumulator) {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{Database: db, RetentionPolicy: rp, Precision: "ns"})
	if err != nil {
		log.Fatal(err)
	}
	for group, v := range classifications {
		p, err := client.NewPoint(
			measurement,
			map[string]string{
				"host":  host,
				"group": group,
			},
			map[string]interface{}{
				"cpuUsage": v.cpuUsage,
				"count":    v.count,
			},
			time.Unix(0, timestamp),
		)
		if err != nil {
			log.Fatal(err)
		}
		bp.AddPoint(p)
	}
	err = c.Write(bp)
	if err != nil {
		log.Fatal(err)
	}
}

// Take a nasty set row of data and turn it into a processMetric (assumes ordering and type of the values in the array)
func toProcessMetric(h string, values []interface{}) (*processMetric, error) {
	tjn, ok := values[0].(json.Number)
	if !ok {
		return nil, fmt.Errorf("%T is not an json.Number", values[0])

	}
	t, e := tjn.Int64()
	if e != nil {
		return nil, e
	}
	ps, ok := values[1].(string)
	if !ok {
		return nil, fmt.Errorf("%T %v is not an string %s", values[1], values[1], values)

	}
	p, e := strconv.Atoi(ps)
	if e != nil {
		return nil, fmt.Errorf("ps is not int %s", e)
	}
	cujn, ok := values[2].(json.Number)
	cu := float64(0)
	if values[2] != nil {
		if !ok {
			return nil, fmt.Errorf("%t %v is not an float64", values[2], values[2])
		}
		cu, e = cujn.Float64()
		if e != nil {
			return nil, fmt.Errorf("cujn is not float64 %s %v", e, values[2])
		}
	}

	u, ok := values[3].(string)
	if !ok {
		return nil, fmt.Errorf("%v is not an string", values[3])
	}
	cl, ok := values[4].(string)
	if !ok {
		return nil, fmt.Errorf("%v is not an string", values[4])
	}
	return &processMetric{
		timestamp: t,
		host:      h,
		pid:       p,
		cpuUsage:  cu,
		user:      u,
		cmdLine:   cl,
	}, nil
}

type accumulator struct {
	count    int
	cpuUsage float64
}

type classifier func(pm *processMetric) (bool, string)

// The classifiers - hard coded at the moment and implemented with pure substring but could be regular expression and in a config file
var classifiers = []classifier{
	func(pm *processMetric) (bool, string) {
		if strings.Contains(pm.cmdLine, "yes 123") {
			return true, "Yes123s"
		}
		return false, ""
	},
	func(pm *processMetric) (bool, string) {
		if strings.Contains(pm.cmdLine, "yes") {
			return true, "YesOther"
		}
		return false, ""
	},
	func(pm *processMetric) (bool, string) {
		return true, "Other"
	},
}

// Peform the classification
func processRawHostMetricsWithSameTimestamp(values []*processMetric) map[string]*accumulator {
	result := make(map[string]*accumulator)
	for _, pm := range values {
		for _, c := range classifiers {
			if ok, group := c(pm); ok {
				if a, ok := result[group]; ok {
					a.count++
					a.cpuUsage += pm.cpuUsage
				} else {
					result[group] = &accumulator{count: 1, cpuUsage: pm.cpuUsage}
				}
				break // don't consider any other groups (definition order prescedence)
			}
		}
	}
	return result
}
