package scenarios

import (
	"gitlab.com/chithanh12/aerospike_sample/models"
	"gitlab.com/chithanh12/aerospike_sample/service"
	"syreclabs.com/go/faker"
)

type Scenario struct {
	as *service.AerospikeService
}

func New(as *service.AerospikeService) *Scenario {
	return &Scenario{as: as}
}

func (se *Scenario) PopulateUserData(n int) {
	users := make([]models.User, 0, n)
	for i := 0; i < n; i++ {
		users = append(users, models.User{
			ID:      faker.Code().Isbn10(),
			Name:    faker.Name().Name(),
			Address: faker.Address().String(),
			Age:     faker.RandomInt(5, 99),
		})
	}

	for _, u := range users {
		se.as.Store(u)
	}
}

func (se *Scenario) PopulateUserDataEvent(uid string) {
	event := models.RandomEvent(uid)
	se.as.PutEvent(uid, event)
}
