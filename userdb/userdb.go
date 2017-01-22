package userdb

import (
	"errors"
	"fmt"

	"github.com/garyburd/redigo/redis"
)

type user struct {
	Email string `redis:"email"`
	Name  string `redis:"name"`
}

type userdb struct {
	redisPool *redis.Pool
}

func (db *userdb) CreateUser(u user) error {
	conn := db.redisPool.Get()
	defer conn.Close()

	id, err := redis.Int64(conn.Do("INCR", "next_user_id"))
	if err != nil {
		return err
	}

	if n, err := redis.Int(conn.Do("HSETNX", "users", u.Email, userID(id))); err != nil {
		return err
	} else if n != 1 {
		return errors.New("db: user already exists")
	}

	if _, err := conn.Do("HMSET",
		redis.Args{}.Add(userID(id)).AddFlat(&u)...); err != nil {
		return err
	}

	return nil
}

func (db *userdb) GetUser(email string) (u user, err error) {
	conn := db.redisPool.Get()
	defer conn.Close()

	id, err := redis.String(conn.Do("HGET", "users", email))
	if err != nil {
		return
	}

	v, err := redis.Values(conn.Do("HGETALL", id))
	if err != nil {
		return
	}

	err = redis.ScanStruct(v, &u)
	return
}

// UpdateUser updates all fields (except Email) of user to match the input u.
func (db *userdb) UpdateUser(u user) error {
	conn := db.redisPool.Get()
	defer conn.Close()

	id, err := redis.String(conn.Do("HGET", "users", u.Email))
	if err != nil {
		if err == redis.ErrNil {
			return fmt.Errorf("db: user does not exist: %v", err)
		}
		return err
	}

	if _, err := conn.Do("HMSET", id, "name", u.Name); err != nil {
		return err
	}

	return nil
}

func (db *userdb) DeleteUser(email string) error {
	conn := db.redisPool.Get()
	defer conn.Close()

	id, err := redis.String(conn.Do("HGET", "users", email))
	if err != nil {
		if err == redis.ErrNil {
			return fmt.Errorf("db: user does not exist: %v", err)
		}
		return err
	}

	if n, err := redis.Int(conn.Do("HDEL", "users", email)); err != nil {
		return err
	} else if n == 0 {
		return errors.New("db: failed to delete user")
	}

	if n, err := redis.Int(conn.Do("DEL", id)); err != nil {
		return err
	} else if n != 1 {
		return errors.New("db: failed to delete user")
	}

	return nil
}

func userID(id int64) string {
	return fmt.Sprintf("user:%d", id)
}
