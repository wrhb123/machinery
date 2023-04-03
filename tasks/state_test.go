package tasks_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wrhb123/machinery/tasks"
)

func TestTaskStateIsCompleted(t *testing.T) {
	t.Parallel()

	taskState := &tasks.TaskState{
		TaskUUID: "taskUUID",
		State:    tasks.StatePending,
	}

	assert.False(t, taskState.IsCompleted())

	taskState.State = tasks.StateReceived
	assert.False(t, taskState.IsCompleted())

	taskState.State = tasks.StateStarted
	assert.False(t, taskState.IsCompleted())

	taskState.State = tasks.StateSuccess
	assert.True(t, taskState.IsCompleted())

	taskState.State = tasks.StateFailure
	assert.True(t, taskState.IsCompleted())
}
