package service

import (
	"fmt"
	"time"

	"gitlab.com/chithanh12/aerospike_sample/models"

	"github.com/aerospike/aerospike-client-go"
	aero "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
	"gitlab.com/chithanh12/aerospike_sample/config"
)

const (
	UserSet  = "users"
	EventSet = "events"
)

var (
	wpolicy = aero.NewWritePolicy(0, 0)
)

type (
	IndexDefinition struct {
		Name string
		Set  string
		Bin  string
		Typ  aerospike.IndexType
	}
	AerospikeService struct {
		cfg    config.AerospikeConfig
		client *aero.Client
	}
)

func NewAerospikeStore(cfg config.AerospikeConfig) *AerospikeService {
	client, err := aero.NewClient(cfg.Host, cfg.Port)
	if err != nil {
		panic("can not connect to aerospike")
	}
	as := &AerospikeService{
		client: client,
		cfg:    cfg,
	}

	as.registerIndexes(
	//IndexDefinition{Name: "age_index", Set: UserSet, Bin: "age", Typ: aerospike.NUMERIC},
	//IndexDefinition{Name: "migrated_idx", Set: UserSet, Bin: "extracted", Typ: aerospike.NUMERIC},
	)

	return as
}

func (as *AerospikeService) GetByAge(minAge int) []models.User {
	stmt := aerospike.NewStatement(as.cfg.Namespace, UserSet, "id", "name", "age", "address")
	recordset, err := as.client.QueryAggregate(nil, stmt, "age_filter", "age_filter", aerospike.NewValue(minAge))
	if err != nil {
		panic(err)
	}

	var result []models.User

	for r := range recordset.Results() {
		if u, ok := r.Record.Bins["SUCCESS"].(map[interface{}]interface{}); ok {
			result = append(result, models.User{
				ID:      u["id"].(string),
				Name:    u["name"].(string),
				Age:     int(u["age"].(int)),
				Address: u["address"].(string),
			})
		}
	}

	return result
}

func (as *AerospikeService) PutEvent(uid string, event models.Event) {
	key, _ := aero.NewKey(as.cfg.Namespace, EventSet, uid)
	addMode := aero.NewMapPolicy(aero.MapOrder.KEY_ORDERED, aero.MapWriteMode.CREATE_ONLY)
	_, err := as.client.Operate(wpolicy, key, aero.MapPutItemsOp(addMode, "event", map[interface{}]interface{}{event.Timestamp: event.ToAerospikeMap()}))

	if err != nil {
		if asErr, ok := err.(types.AerospikeError); ok && asErr.ResultCode() == types.FAIL_ELEMENT_EXISTS {
			fmt.Printf("%v\n", err)
			return
		}

		panic(err)
	}
}

func (as *AerospikeService) PutEvents(uid string, events []models.Event) {
	key, _ := aero.NewKey(as.cfg.Namespace, EventSet, uid)
	addMode := aero.NewMapPolicy(aero.MapOrder.KEY_ORDERED, aero.MapWriteMode.UPDATE)
	data := make(map[interface{}]interface{})
	for _, e := range events {
		data[e.Timestamp] = e.ToAerospikeMap()
	}

	_, err := as.client.Operate(wpolicy, key, aero.MapPutItemsOp(addMode, "event", data))

	if err != nil {
		if asErr, ok := err.(types.AerospikeError); ok && asErr.ResultCode() == types.FAIL_ELEMENT_EXISTS {
			fmt.Printf("%v\n", err)
			return
		}

		panic(err)
	}
}
func (as *AerospikeService) GetTopRecentEvent(uid string, limit int) map[int]*models.Event {
	key, _ := aero.NewKey(as.cfg.Namespace, EventSet, uid)

	op := aero.MapGetByIndexRangeCountOp("event", -limit, limit, aero.MapReturnType.KEY_VALUE|aero.MapReturnType.INVERTED)
	r, err := as.client.Operate(wpolicy, key, op)
	if err != nil {
		panic(err)
	}

	items := r.Bins["event"].([]aero.MapPair)
	result := make(map[int]*models.Event, 0)

	for _, v := range items {
		value := v.Value.([]interface{})
		e := &models.Event{}
		e.FromMap(value)
		result[v.Key.(int)] = e
	}
	return result
}

func (as *AerospikeService) GetEventsByEventName(uid, eventName string) map[int]interface{} {
	key, _ := aero.NewKey(as.cfg.Namespace, EventSet, uid)

	op := aero.MapGetByValueOp("event", []interface{}{eventName, aerospike.NewWildCardValue()}, aero.MapReturnType.KEY_VALUE)
	r, err := as.client.Operate(wpolicy, key, op)
	if err != nil {
		panic(err)
	}

	items := r.Bins["event"].([]aero.MapPair)
	result := make(map[int]interface{}, 0)

	for _, v := range items {
		value := v.Value.([]interface{})
		e := &models.Event{}
		e.FromMap(value)
		result[v.Key.(int)] = e
	}
	return result
}

func (as *AerospikeService) Store(u models.User) error {
	key, _ := aero.NewKey(as.cfg.Namespace, UserSet, u.ID)
	return as.client.PutObject(nil, key, u)
}

func (as *AerospikeService) RegisterUdf(udfPath, filename string) {
	aero.SetLuaPath(udfPath)

	regTask, err := as.client.RegisterUDFFromFile(nil, udfPath+filename+".lua", filename+".lua", aero.LUA)
	if err != nil {
		panic(err)
	}
	<-regTask.OnComplete()
}

func (as *AerospikeService) RunFilter() {
	qPolicy := aerospike.NewQueryPolicy()
	qPolicy.UseCompression = true
	qPolicy.TotalTimeout = 30 * time.Second
	qPolicy.MaxRetries = 0

	// create aerospike statement
	stmt := aerospike.NewStatement(as.cfg.Namespace, UserSet, "id", "name", "age")

	var exps []*aerospike.FilterExpression
	exps = append(exps, aerospike.ExpNot(aerospike.ExpBinExists("extracted")))
	exps = append(exps, aerospike.ExpEq(aerospike.ExpIntBin("age"), aerospike.ExpIntVal(20)))
	qPolicy.FilterExpression = aerospike.ExpAnd(exps...)

	// exec aerospike udf
	rs, err := as.client.Query(qPolicy, stmt)
	defer rs.Close()
	if err != nil {
		panic(err)
	}

	for r := range rs.Results() {
		if r.Err != nil {
			panic(r.Err)
		}
		if name, ok := r.Record.Bins["name"].(string); ok {
			fmt.Printf("name = %v, age = %v \n", name, r.Record.Bins["age"].(int))
		}
	}
}

func (as *AerospikeService) registerIndexes(indexes ...IndexDefinition) {
	for _, idx := range indexes {
		task, err := as.client.CreateIndex(nil, as.cfg.Namespace, idx.Set, idx.Name, idx.Bin, idx.Typ)
		if err != nil {
			if asErr, ok := err.(types.AerospikeError); ok && asErr.ResultCode() == types.INDEX_FOUND {
				continue
			}
			panic(err)
		}
		<-task.OnComplete()
	}
}

func (as *AerospikeService) TruncateSet(setName string) error {
	now := time.Now()
	return as.client.Truncate(nil, as.cfg.Namespace, setName, &now)
}
