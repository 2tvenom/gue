package guex

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/2tvenom/guex/database"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func addTasks(client *Client, queue, jobType string, count int) error {
	var jobs []*Job
	for i := 0; i < count; i++ {
		jobs = append(jobs, &Job{
			Job: database.Job{
				Queue:   queue,
				JobType: jobType,
				Payload: []byte(fmt.Sprintf("queue: %s, job: %s", queue, jobType)),
			},
		})
	}

	return client.EnqueueBatch(context.Background(), jobs)
}

func Test_WorkerPool(t *testing.T) {
	var (
		err            error
		connPoolConfig *pgxpool.Config
	)

	connPoolConfig, err = pgxpool.ParseConfig(os.Getenv("DB_DSN"))
	assert.NoError(t, err)

	var pool *pgxpool.Pool
	pool, err = pgxpool.ConnectConfig(context.Background(), connPoolConfig)
	assert.NoError(t, err)

	_, err = pool.Exec(context.Background(), "TRUNCATE TABLE _jobs")
	assert.NoError(t, err)

	var client = NewClient(pool)
	addTasks(client, "foo", "foo", 100)
	addTasks(client, "bar", "bar", 50)
	addTasks(client, "bar", "baz", 50)

	var logger *zap.Logger
	logger, err = zap.NewDevelopment()
	assert.NoError(t, err)

	var (
		wp *WorkerPool
	)
	rand.Seed(time.Now().Unix())
	wp, err = NewWorkerPool(
		client,
		WithPoolInterval(time.Second/2),
		WithWorkerPoolQueue(QueueLimit{
			Queue: "foo",
			Limit: 1,
		}, QueueLimit{
			Queue: "bar",
			Limit: 10,
		}),
		WithWorkerPoolHandler("foo", func(ctx context.Context, j *Job) error {
			time.Sleep(time.Second * 3)
			return fmt.Errorf("some err")
		}),
		WithLogger(logger),
		WithWorkerPoolHandler("bar", func(ctx context.Context, j *Job) error {
			//fmt.Println(string(j.Payload))
			time.Sleep(time.Second * time.Duration(rand.Int31n(5)))
			return nil
		}),
		WithWorkerPoolHandler("baz", func(ctx context.Context, j *Job) error {
			//fmt.Println(string(j.Payload))
			time.Sleep(time.Second * time.Duration(rand.Int31n(5)))
			return nil
		}),
	)

	assert.NoError(t, err)

	var ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	err = wp.Run(ctx)
}
