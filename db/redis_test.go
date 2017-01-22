package db

import (
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestCRUD(t *testing.T) {
	assert := assert.New(t)

	p := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", ":6379") },
	}
	defer flushAll(p)

	db := &userdb{
		redisPool: p,
	}

	testEmail := "user@example.com"
	testName := "Test User"
	testUser := user{
		Email: testEmail,
		Name:  testName,
	}
	assert.NoError(db.CreateUser(testUser))
	assert.Error(db.CreateUser(testUser))

	actual, err := db.GetUser(testEmail)
	assert.NoError(err)
	assert.Equal(testUser, actual)

	testUser.Name = "New User Name"
	assert.NoError(db.UpdateUser(testUser))

	actual, err = db.GetUser(testEmail)
	assert.NoError(err)
	assert.Equal(testUser, actual)

	testUser.Email = "newuser@example.com"
	assert.Error(db.UpdateUser(testUser))

	assert.NoError(db.DeleteUser(testEmail))
	assert.Error(db.DeleteUser(testEmail))
}

func flushAll(p *redis.Pool) (err error) {
	conn := p.Get()
	defer conn.Close()

	_, err = conn.Do("FLUSHALL")
	return
}
