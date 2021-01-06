package models

import (
	"time"

	"syreclabs.com/go/faker"
)

type Action string

const (
	ViewAction = Action("view")
	PlayAction = Action("play")
	BuyAction  = Action("buy")
)

type Event struct {
	Timestamp   int64  `as:"ts"`
	Item        string `as:"item"`
	UserID      string `as:"user_id"`
	Action      Action `as:"action"`
	Description string `as:"desc"`
	Count       int    `as:"count"`
}

func (e Event) ToAerospikeMap() []interface{} {
	return []interface{}{
		string(e.Action),
		map[interface{}]interface{}{
			"i": e.Item,
			"d": e.Description,
			"c": e.Count,
		},
	}
}

func (e *Event) FromMap(src1 []interface{}) {
	if len(src1) < 2 {
		return
	}

	if a, ok := src1[0].(string); ok {
		e.Action = Action(a)
	}
	src := src1[1].(map[interface{}]interface{})
	if item, ok := src["i"].(string); ok {
		e.Item = item
	}

	if desc, ok := src["d"].(string); ok {
		e.Description = desc
	}

	if c, ok := src["c"].(int); ok {
		e.Count = c
	}
}

func RandomEvent(uid string) Event {
	ts := faker.Time().Between(time.Now().AddDate(-1, 0, 0), time.Now()).UnixNano() / 1e6
	action := faker.RandomChoice([]string{string(ViewAction), string(PlayAction), string(BuyAction)})

	return Event{
		Timestamp:   ts,
		UserID:      uid,
		Item:        faker.RandomString(16),
		Action:      Action(action),
		Description: faker.RandomString(20),
		Count:       faker.RandomInt(1, 100),
	}
}
