package integration_test

import (
	"os"
	"testing"

	"github.com/wrhb123/machinery"
	"github.com/wrhb123/machinery/config"

	amqpbackend "github.com/wrhb123/machinery/backends/amqp"
	amqpbroker "github.com/wrhb123/machinery/brokers/amqp"
	eagerlock "github.com/wrhb123/machinery/locks/eager"
)

func TestAmqpAmqp(t *testing.T) {
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		t.Skip("AMQP_URL is not defined")
	}

	finalAmqpURL := amqpURL
	var finalSeparator string

	amqpURLs := os.Getenv("AMQP_URLS")
	if amqpURLs != "" {
		separator := os.Getenv("AMQP_URLS_SEPARATOR")
		if separator == "" {
			return
		}
		finalSeparator = separator
		finalAmqpURL = amqpURLs
	}

	cnf := &config.Config{
		Broker:                  finalAmqpURL,
		MultipleBrokerSeparator: finalSeparator,
		DefaultQueue:            "machinery_tasks",
		ResultBackend:           amqpURL,
		ResultsExpireIn:         3600,
		AMQP: &config.AMQPConfig{
			Exchange:      "test_exchange",
			ExchangeType:  "direct",
			BindingKey:    "test_task",
			PrefetchCount: 1,
		},
	}

	broker := amqpbroker.New(cnf)
	backend := amqpbackend.New(cnf)
	lock := eagerlock.New()
	server := machinery.NewServer(cnf, broker, backend, lock)

	registerTestTasks(server)

	worker := server.NewWorker("test_worker", 0)
	defer worker.Quit()
	go worker.Launch()
	testAll(server, t)
}
