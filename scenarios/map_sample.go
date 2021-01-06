package scenarios

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"gitlab.com/chithanh12/aerospike_sample/models"

	"syreclabs.com/go/faker"
)

const (
	MAX_USERS_COUNT = 500000
	POPULATE_THREAD = 100
	EVENTS_PER_USER = 150
)

func (s *Scenario) PopulateLargeEvent() {
	max := MAX_USERS_COUNT / POPULATE_THREAD
	var wg sync.WaitGroup
	wg.Add(POPULATE_THREAD)

	for t := 0; t < POPULATE_THREAD; t++ {
		go func(tn int) {
			defer wg.Done()

			for i := 0; i < max; i++ {
				start := time.Now().UnixNano() / 1e6
				events := make([]models.Event, 0, 200)
				uid := fmt.Sprintf("user%v", tn*max+i)
				for j := 0; j < EVENTS_PER_USER; j++ {
					events = append(events, models.RandomEvent(uid))
				}

				s.as.PutEvents(uid, events)
				end := time.Now().UnixNano() / 1e6
				fmt.Printf("populate duration = %v ms\n", end-start)
			}
		}(t)
	}

	wg.Wait()
}

func (s *Scenario) RunMapSample() {
	uid := faker.RandomString(10)
	max := 1000

	for i := 0; i < max; i++ {
		s.PopulateUserDataEvent(uid)
	}

	item := s.as.GetEventsByEventName(uid, "play")
	str, _ := json.MarshalIndent(item, "", "  ")
	fmt.Println(string(str))
}

func (s *Scenario) MapWithRangeSample() {
	uid := faker.RandomString(10)
	max := 1000

	for i := 0; i < max; i++ {
		s.PopulateUserDataEvent(uid)
	}
	start := time.Now().UnixNano()
	item := s.as.GetTopRecentEvent(uid, 10)
	end := time.Now().UnixNano()
	str, _ := json.MarshalIndent(item, "", "  ")
	fmt.Printf("Duration = %vns\n", end-start)
	fmt.Println(string(str))
}

func (s *Scenario) BenchMarkMapGet() {
	sample := 1

	for i := 0; i < sample; i++ {
		uid := fmt.Sprintf("user%v", 1)

		start := time.Now().UnixNano() / 1e6
		item := s.as.GetTopRecentEvent(uid, 100)

		str, _ := json.Marshal(item)
		fmt.Printf("%v", string(str))

		e := time.Now().UnixNano() / 1e6
		fmt.Printf("%v ms\n", e-start)
	}
}
