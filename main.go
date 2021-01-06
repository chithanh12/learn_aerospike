package main

import (
	"fmt"

	"gitlab.com/chithanh12/aerospike_sample/scenarios"

	"gitlab.com/chithanh12/aerospike_sample/config"
	"gitlab.com/chithanh12/aerospike_sample/service"
)

var (
	cfg         config.AerospikeConfig
	aeroService *service.AerospikeService
	scenario    *scenarios.Scenario
)

func init() {
	cfg = config.AerospikeConfig{
		Host:      "localhost",
		Port:      3000,
		Namespace: "test",
		Udf:       "./udf/",
	}
	aeroService = service.NewAerospikeStore(cfg)
	aeroService.RegisterUdf("./udf/", "age_filter")
	scenario = scenarios.New(aeroService)
}

func main() {
	fmt.Println("Init success")

	//// Generate events
	//start := time.Now().UnixNano() / 1e6
	//scenario.PopulateLargeEvent()
	//end := time.Now().UnixNano() / 1e6
	//fmt.Printf("Duration = %v ms", end-start)

	//
	//aeroService.RunFilter()

	//
	//scenario.RunMapSample()
	//scenario.MapWithRangeSample()

	// Check performance for get item from maps
	scenario.BenchMarkMapGet()
}
