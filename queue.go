package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketPending    = []byte("pending")
	bucketDeadLetter = []byte("dead_letter")
)

// TaskKind describes the type of replication operation.
type TaskKind string

const (
	TaskCopy   TaskKind = "copy"   // copy an object from src → dst
	TaskDelete TaskKind = "delete" // delete an object on a backend
)

// ReplicationTask is the unit of work stored in the queue.
type ReplicationTask struct {
	ID          string    `json:"id"`
	Kind        TaskKind  `json:"kind"`
	SrcBackend  string    `json:"src_backend"`
	DstBackend  string    `json:"dst_backend"`
	Bucket      string    `json:"bucket"`       // logical bucket name (client-facing)
	ObjectKey   string    `json:"object_key"`
	Attempts    int       `json:"attempts"`
	LastAttempt time.Time `json:"last_attempt"`
	CreatedAt   time.Time `json:"created_at"`
}

// Queue is a persistent, disk-backed FIFO powered by BoltDB.
// Tasks survive process restarts and are retried up to RetryLimit times
// before being moved to a dead-letter bucket for manual inspection.
type Queue struct {
	db         *bolt.DB
	cfg        QueueConfig
	muEnqueue  sync.Mutex
}

// OpenQueue opens (or creates) the BoltDB database and initialises the
// required buckets.
func OpenQueue(cfg QueueConfig) (*Queue, error) {
	db, err := bolt.Open(cfg.DBPath, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("queue: cannot open bolt db at %q: %w", cfg.DBPath, err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketPending); err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(bucketDeadLetter)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("queue: cannot init buckets: %w", err)
	}

	log.Printf("[queue] opened %s  (workers=%d, retry_limit=%d)", cfg.DBPath, cfg.Workers, cfg.RetryLimit)
	return &Queue{db: db, cfg: cfg}, nil
}

// Close closes the underlying BoltDB database.
func (q *Queue) Close() error {
	return q.db.Close()
}

// Enqueue adds a new ReplicationTask to the pending queue.
func (q *Queue) Enqueue(task *ReplicationTask) error {
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}

	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("queue: cannot marshal task: %w", err)
	}

	q.muEnqueue.Lock()
	defer q.muEnqueue.Unlock()

	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketPending)
		// Use the task ID as the key so updates are idempotent.
		return b.Put([]byte(task.ID), data)
	})
}

// Dequeue atomically reads and locks the next pending task for processing.
// It does NOT remove the task from the DB until Ack or Nack is called, so
// a crash during processing will leave the task in the queue for retry.
func (q *Queue) Dequeue() (*ReplicationTask, error) {
	var task *ReplicationTask

	err := q.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketPending)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			t := &ReplicationTask{}
			if err := json.Unmarshal(v, t); err != nil {
				continue
			}

			// Check if this task is ready based on exponential back-off
			if t.Attempts > 0 {
				backoff := q.cfg.RetryBackoff * (1 << uint(t.Attempts-1))
				if time.Since(t.LastAttempt) < backoff {
					continue // skip this task for now; still in back-off
				}
			}

			// Found a task that is ready to be retried or is new
			task = t
			break
		}
		return nil
	})
	return task, err
}

// Ack removes a successfully processed task from the queue.
func (q *Queue) Ack(taskID string) error {
	return q.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketPending).Delete([]byte(taskID))
	})
}

// Nack increments the attempt counter. If the task has exceeded RetryLimit,
// it is moved to the dead-letter bucket; otherwise it stays in pending with
// an updated attempt count so it will be retried after the back-off.
func (q *Queue) Nack(task *ReplicationTask, reason error) error {
	task.Attempts++
	task.LastAttempt = time.Now()

	if task.Attempts >= q.cfg.RetryLimit {
		log.Printf("[queue] task %s exceeded retry limit (%d), moving to dead-letter", task.ID, q.cfg.RetryLimit)
		return q.db.Update(func(tx *bolt.Tx) error {
			data, err := json.Marshal(task)
			if err != nil {
				return err
			}
			if err := tx.Bucket(bucketDeadLetter).Put([]byte(task.ID), data); err != nil {
				return err
			}
			return tx.Bucket(bucketPending).Delete([]byte(task.ID))
		})
	}

	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return q.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketPending).Put([]byte(task.ID), data)
	})
}

// PendingCount returns the number of tasks currently in the pending bucket.
func (q *Queue) PendingCount() (int, error) {
	var count int
	err := q.db.View(func(tx *bolt.Tx) error {
		count = tx.Bucket(bucketPending).Stats().KeyN
		return nil
	})
	return count, err
}

// ReplicationWorker drains the queue by executing replication tasks
// against the actual S3 backends.
type ReplicationWorker struct {
	queue    *Queue
	backends map[string]*Backend
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewReplicationWorker creates a worker pool backed by the given queue.
func NewReplicationWorker(q *Queue, backends []*Backend) *ReplicationWorker {
	bm := make(map[string]*Backend, len(backends))
	for _, b := range backends {
		bm[b.Config.Name] = b
	}
	return &ReplicationWorker{
		queue:    q,
		backends: bm,
		stopCh:   make(chan struct{}),
	}
}

// Start launches cfg.Workers goroutines.
func (rw *ReplicationWorker) Start(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		rw.wg.Add(1)
		go func(id int) {
			defer rw.wg.Done()
			rw.workerLoop(id)
		}(i)
	}
	log.Printf("[queue] %d replication worker(s) started", numWorkers)
}

// Stop signals workers to exit gracefully and waits.
func (rw *ReplicationWorker) Stop() {
	close(rw.stopCh)
	rw.wg.Wait()
}

func (rw *ReplicationWorker) workerLoop(id int) {
	for {
		select {
		case <-rw.stopCh:
			return
		default:
		}

		task, err := rw.queue.Dequeue()
		if err != nil {
			log.Printf("[worker-%d] dequeue error: %v", id, err)
			time.Sleep(2 * time.Second)
			continue
		}
		if task == nil {
			// Queue is empty — sleep briefly before polling again.
			time.Sleep(500 * time.Millisecond)
			continue
		}


		log.Printf("[worker-%d] executing task %s kind=%s key=%s", id, task.ID, task.Kind, task.ObjectKey)
		if err := rw.execute(task); err != nil {
			log.Printf("[worker-%d] task %s failed (attempt %d): %v", id, task.ID, task.Attempts+1, err)
			_ = rw.queue.Nack(task, err)
		} else {
			_ = rw.queue.Ack(task.ID)
		}
	}
}

func (rw *ReplicationWorker) execute(task *ReplicationTask) error {
	ctx := context.Background()

	switch task.Kind {
	case TaskCopy:
		src, ok := rw.backends[task.SrcBackend]
		if !ok {
			return fmt.Errorf("unknown source backend %q", task.SrcBackend)
		}
		dst, ok := rw.backends[task.DstBackend]
		if !ok {
			return fmt.Errorf("unknown destination backend %q", task.DstBackend)
		}
		return copyObject(ctx, src, dst, task.ObjectKey)

	case TaskDelete:
		dst, ok := rw.backends[task.DstBackend]
		if !ok {
			return fmt.Errorf("unknown destination backend %q", task.DstBackend)
		}
		inp := deleteObjectInput(dst, task.ObjectKey)
		_, err := dst.Client.DeleteObject(ctx, &inp)
		return err

	default:
		return fmt.Errorf("unknown task kind %q", task.Kind)
	}
}
