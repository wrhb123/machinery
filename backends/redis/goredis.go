package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	redsyncgoredis "github.com/go-redsync/redsync/v4/redis/goredis/v8"

	"github.com/wrhb123/machinery/backends/iface"
	"github.com/wrhb123/machinery/common"
	"github.com/wrhb123/machinery/config"
	"github.com/wrhb123/machinery/log"
	"github.com/wrhb123/machinery/tasks"
)

// BackendGR represents a Redis result backend
type BackendGR struct {
	common.Backend
	rclient   redis.UniversalClient
	host      string
	username  string
	password  string
	db        int
	redsync   *redsync.Redsync
	redisOnce sync.Once
}

// NewGR creates Backend instance
func NewGR(cnf *config.Config, addrs []string, username, password string, db int) iface.Backend {
	b := &BackendGR{
		Backend: common.NewBackend(cnf),
	}

	b.username = username
	b.password = password

	ropt := &redis.UniversalOptions{
		Addrs:        addrs,
		DB:           db,
		Username:     b.username,
		Password:     b.password,
		PoolSize:     15,
		ReadTimeout:  time.Duration(cnf.Redis.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(cnf.Redis.WriteTimeout) * time.Second,
		DialTimeout:  time.Duration(cnf.Redis.ConnectTimeout) * time.Second,
	}

	if cnf.Redis != nil {
		ropt.MasterName = cnf.Redis.MasterName
	}

	b.rclient = redis.NewUniversalClient(ropt)
	b.redsync = redsync.New(redsyncgoredis.NewPool(b.rclient))
	return b
}

// InitGroup creates and saves a group meta data object
func (b *BackendGR) InitGroup(groupUUID string, taskUUIDs []string) error {
	groupMeta := &tasks.GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
		CreatedAt: time.Now().UTC(),
	}

	encoded, err := json.Marshal(groupMeta)
	if err != nil {
		return err
	}

	expiration := b.getExpiration()
	err = b.rclient.Set(context.Background(), groupUUID, encoded, expiration).Err()
	if err != nil {
		return err
	}

	return nil
}

// GroupCompleted returns true if all tasks in a group finished
func (b *BackendGR) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}

	var countSuccessTasks = 0
	for _, taskState := range taskStates {
		if taskState.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *BackendGR) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return []*tasks.TaskState{}, err
	}

	return b.getStates(groupMeta.TaskUUIDs...)
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *BackendGR) TriggerChord(groupUUID string) (bool, error) {
	m := b.redsync.NewMutex("TriggerChordMutex")
	if err := m.Lock(); err != nil {
		return false, err
	}
	defer m.Unlock()

	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	// Chord has already been triggered, return false (should not trigger again)
	if groupMeta.ChordTriggered {
		return false, nil
	}

	// Set flag to true
	groupMeta.ChordTriggered = true

	// Update the group meta
	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return false, err
	}

	expiration := b.getExpiration()
	err = b.rclient.Set(context.Background(), groupUUID, encoded, expiration).Err()
	if err != nil {
		return false, err
	}

	return true, nil
}

func (b *BackendGR) mergeNewTaskState(newState *tasks.TaskState) {
	state, err := b.GetState(newState.TaskUUID)
	if err == nil {
		newState.CreatedAt = state.CreatedAt
		newState.TaskName = state.TaskName
	}
}

// SetStatePending updates task state to PENDING
func (b *BackendGR) SetStatePending(signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	return b.updateState(taskState)
}

// SetStateReceived updates task state to RECEIVED
func (b *BackendGR) SetStateReceived(signature *tasks.Signature) error {
	taskState := tasks.NewReceivedTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateStarted updates task state to STARTED
func (b *BackendGR) SetStateStarted(signature *tasks.Signature) error {
	taskState := tasks.NewStartedTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateRetry updates task state to RETRY
func (b *BackendGR) SetStateRetry(signature *tasks.Signature) error {
	taskState := tasks.NewRetryTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateSuccess updates task state to SUCCESS
func (b *BackendGR) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	taskState := tasks.NewSuccessTaskState(signature, results)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateFailure updates task state to FAILURE
func (b *BackendGR) SetStateFailure(signature *tasks.Signature, err string) error {
	taskState := tasks.NewFailureTaskState(signature, err)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// GetState returns the latest task state
func (b *BackendGR) GetState(taskUUID string) (*tasks.TaskState, error) {

	item, err := b.rclient.Get(context.Background(), taskUUID).Bytes()
	if err != nil {
		return nil, err
	}
	state := new(tasks.TaskState)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, err
	}

	return state, nil
}

// PurgeState deletes stored task state
func (b *BackendGR) PurgeState(taskUUID string) error {
	err := b.rclient.Del(context.Background(), taskUUID).Err()
	if err != nil {
		return err
	}

	return nil
}

// PurgeGroupMeta deletes stored group meta data
func (b *BackendGR) PurgeGroupMeta(groupUUID string) error {
	err := b.rclient.Del(context.Background(), groupUUID).Err()
	if err != nil {
		return err
	}

	return nil
}

// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *BackendGR) getGroupMeta(groupUUID string) (*tasks.GroupMeta, error) {
	item, err := b.rclient.Get(context.Background(), groupUUID).Bytes()
	if err != nil {
		return nil, err
	}

	groupMeta := new(tasks.GroupMeta)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(groupMeta); err != nil {
		return nil, err
	}

	return groupMeta, nil
}

// getStates returns multiple task states
func (b *BackendGR) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	taskStates := make([]*tasks.TaskState, len(taskUUIDs))
	// to avoid CROSSSLOT error, use pipeline
	cmders, err := b.rclient.Pipelined(context.Background(), func(pipeliner redis.Pipeliner) error {
		for _, uuid := range taskUUIDs {
			pipeliner.Get(context.Background(), uuid)
		}
		return nil
	})
	if err != nil {
		return taskStates, err
	}
	for i, cmder := range cmders {
		stateBytes, err1 := cmder.(*redis.StringCmd).Bytes()
		if err1 != nil {
			return taskStates, err1
		}
		taskState := new(tasks.TaskState)
		decoder := json.NewDecoder(bytes.NewReader(stateBytes))
		decoder.UseNumber()
		if err1 = decoder.Decode(taskState); err1 != nil {
			log.ERROR.Print(err1)
			return taskStates, err1
		}
		taskStates[i] = taskState
	}

	return taskStates, nil
}

// updateState saves current task state
func (b *BackendGR) updateState(taskState *tasks.TaskState) error {
	encoded, err := json.Marshal(taskState)
	if err != nil {
		return err
	}

	expiration := b.getExpiration()
	_, err = b.rclient.Set(context.Background(), taskState.TaskUUID, encoded, expiration).Result()
	if err != nil {
		return err
	}

	return nil
}

// getExpiration returns expiration for a stored task state
func (b *BackendGR) getExpiration() time.Duration {
	expiresIn := b.GetConfig().ResultsExpireIn
	if expiresIn == 0 {
		// expire results after 1 hour by default
		expiresIn = config.DefaultResultsExpireIn
	}

	return time.Duration(expiresIn) * time.Second
}
