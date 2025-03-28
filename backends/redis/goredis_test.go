package redis_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/wrhb123/machinery/backends/iface"

	redislib "github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/wrhb123/machinery/backends/redis"
	"github.com/wrhb123/machinery/config"
	"github.com/wrhb123/machinery/tasks"
)

func getRedisG() iface.Backend {
	// host1:port1,host2:port2
	redisURL := os.Getenv("REDIS_URL_GR")
	redisUser := os.Getenv("REDIS_USER")
	redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisURL == "" {
		return nil
	}
	backend := redis.NewGR(new(config.Config), []string{redisURL, redisURL}, redisUser, redisPassword, 0)
	return backend
}

func TestGroupCompletedGR(t *testing.T) {
	backend := getRedisG()
	if backend == nil {
		t.Skip()
	}

	groupUUID := "testGroupUUID"
	task1 := &tasks.Signature{
		UUID:      "testTaskUUID1",
		GroupUUID: groupUUID,
	}
	task2 := &tasks.Signature{
		UUID:      "testTaskUUID2",
		GroupUUID: groupUUID,
	}

	// Cleanup before the test
	backend.PurgeState(task1.UUID)
	backend.PurgeState(task2.UUID)
	backend.PurgeGroupMeta(groupUUID)

	groupCompleted, err := backend.GroupCompleted(groupUUID, 2)
	if assert.Error(t, err) {
		assert.False(t, groupCompleted)
		assert.Equal(t, "redis: nil", err.Error())
	}

	backend.InitGroup(groupUUID, []string{task1.UUID, task2.UUID})

	groupCompleted, err = backend.GroupCompleted(groupUUID, 2)
	if assert.Error(t, err) {
		assert.False(t, groupCompleted)
		assert.Equal(t, "redis: nil", err.Error())
	}

	backend.SetStatePending(task1)
	backend.SetStateStarted(task2)
	groupCompleted, err = backend.GroupCompleted(groupUUID, 2)
	if assert.NoError(t, err) {
		assert.False(t, groupCompleted)
	}

	taskResults := []*tasks.TaskResult{new(tasks.TaskResult)}
	backend.SetStateStarted(task1)
	backend.SetStateSuccess(task2, taskResults)
	groupCompleted, err = backend.GroupCompleted(groupUUID, 2)
	if assert.NoError(t, err) {
		assert.False(t, groupCompleted)
	}

	backend.SetStateFailure(task1, "Some error")
	groupCompleted, err = backend.GroupCompleted(groupUUID, 2)
	if assert.NoError(t, err) {
		assert.True(t, groupCompleted)
	}
}

func TestGetStateGR(t *testing.T) {
	backend := getRedisG()
	if backend == nil {
		t.Skip()
	}

	signature := &tasks.Signature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend.PurgeState("testTaskUUID")

	var (
		taskState *tasks.TaskState
		err       error
	)

	taskState, err = backend.GetState(signature.UUID)
	assert.Equal(t, "redis: nil", err.Error())
	assert.Nil(t, taskState)

	//Pending State
	backend.SetStatePending(signature)
	taskState, err = backend.GetState(signature.UUID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	createdAt := taskState.CreatedAt

	//Received State
	backend.SetStateReceived(signature)
	taskState, err = backend.GetState(signature.UUID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	assert.Equal(t, createdAt, taskState.CreatedAt)

	//Started State
	backend.SetStateStarted(signature)
	taskState, err = backend.GetState(signature.UUID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	assert.Equal(t, createdAt, taskState.CreatedAt)

	//Success State
	taskResults := []*tasks.TaskResult{
		{
			Type:  "float64",
			Value: 2,
		},
	}
	backend.SetStateSuccess(signature, taskResults)
	taskState, err = backend.GetState(signature.UUID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	assert.Equal(t, createdAt, taskState.CreatedAt)
	assert.NotNil(t, taskState.Results)
}

func TestPurgeStateGR(t *testing.T) {
	backend := getRedisG()
	if backend == nil {
		t.Skip()
	}

	signature := &tasks.Signature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend.SetStatePending(signature)
	taskState, err := backend.GetState(signature.UUID)
	assert.NotNil(t, taskState)
	assert.NoError(t, err)

	backend.PurgeState(taskState.TaskUUID)
	taskState, err = backend.GetState(signature.UUID)
	assert.Nil(t, taskState)
	assert.Error(t, err)
}

func TestRpush(t *testing.T) {
	redisAddr := "r-uf65egew9whkeryiqxpi.redis.rds.aliyuncs.com:6379"

	rclient := redislib.NewUniversalClient(&redislib.UniversalOptions{
		Addrs:        []string{redisAddr, redisAddr},
		Username:     "lb",
		Password:     "lb@123456",
		PoolSize:     15,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		DialTimeout:  5 * time.Second,
	})
	//a := rclient.RPush(context.Background(), "mmmt", "task-xxx-yyy-zzz")
	//fmt.Println(a.Result())
	//rclient.Set(context.Background(), "aaa", "bbb", -1)
	mmmt := rclient.LRange(context.Background(), "mmmt", 0, -1)
	fmt.Println(mmmt.Result())
}
