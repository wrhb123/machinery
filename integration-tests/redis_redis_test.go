package integration_test

import (
	"os"
	"testing"

	"github.com/wrhb123/machinery"
	redisbackend "github.com/wrhb123/machinery/backends/redis"
	redisbroker "github.com/wrhb123/machinery/brokers/redis"
	"github.com/wrhb123/machinery/config"
	eagerlock "github.com/wrhb123/machinery/locks/eager"
)

func TestRedisRedis_GoRedis(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	redisUser := os.Getenv("REDIS_USER")
	redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	cnf := &config.Config{
		DefaultQueue:    "machinery_tasks",
		ResultsExpireIn: 3600,
		Redis: &config.RedisConfig{
			MaxIdle:                3,
			IdleTimeout:            240,
			ReadTimeout:            15,
			WriteTimeout:           15,
			ConnectTimeout:         15,
			NormalTasksPollPeriod:  1000,
			DelayedTasksPollPeriod: 500,
		},
	}

	broker := redisbroker.NewGR(cnf, []string{redisURL}, redisUser, redisPassword, 0)
	backend := redisbackend.NewGR(cnf, []string{redisURL}, redisUser, redisPassword, 0)
	lock := eagerlock.New()
	server := machinery.NewServer(cnf, broker, backend, lock)

	registerTestTasks(server)

	worker := server.NewWorker("test_worker", 0)
	defer worker.Quit()
	go worker.Launch()
	testAll(server, t)
}
