package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Status string

const (
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

type Job struct {
	ID        string    `json:"id"`
	Payload   string    `json:"payload"`
	Status    Status    `json:"status"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type Processor interface {
	Process(ctx context.Context, payload string) error
}

type Service struct {
	mu        sync.RWMutex
	jobs      map[string]*Job
	queue     chan string
	workers   int
	processor Processor
	stopping  atomic.Bool
	stopOnce  sync.Once
	wg        sync.WaitGroup
	sequence  atomic.Uint64
}

func NewService(workers, queueCapacity int) *Service {
	if workers < 1 {
		workers = 1
	}
	if queueCapacity < 1 {
		queueCapacity = 1
	}

	s := &Service{
		jobs:      make(map[string]*Job),
		queue:     make(chan string, queueCapacity),
		workers:   workers,
		processor: ProcessorFunc(defaultProcessor),
	}
	s.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go s.worker()
	}
	return s
}

type ProcessorFunc func(context.Context, string) error

func (f ProcessorFunc) Process(ctx context.Context, payload string) error {
	return f(ctx, payload)
}

func defaultProcessor(ctx context.Context, payload string) error {
	select {
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	if payload == "" {
		return errors.New("empty payload")
	}
	if containsFail(payload) {
		return errors.New("simulated processing failure")
	}
	return nil
}

func containsFail(s string) bool {
	return strings.Contains(s, "fail")
}

func (s *Service) Create(ctx context.Context, payload string) (*Job, error) {
	if strings.TrimSpace(payload) == "" {
		return nil, errors.New("payload is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopping.Load() {
		return nil, errors.New("service is stopping")
	}

	id := fmt.Sprintf("job-%d", s.sequence.Add(1))
	job := &Job{
		ID:        id,
		Payload:   payload,
		Status:    StatusQueued,
		CreatedAt: time.Now().UTC(),
	}

	if _, exists := s.jobs[id]; exists {
		return nil, errors.New("job already exists")
	}

	s.jobs[id] = job
	select {
	case s.queue <- id:
		return cloneJob(job), nil
	case <-ctx.Done():
		delete(s.jobs, id)
		return nil, ctx.Err()
	default:
		delete(s.jobs, id)
		return nil, errors.New("job queue is full")
	}
}

func (s *Service) Get(id string) (*Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	if !ok {
		return nil, false
	}
	copy := *job
	return &copy, true
}

func (s *Service) worker() {
	defer s.wg.Done()
	for id := range s.queue {
		s.process(id)
	}
}

func (s *Service) process(id string) {
	s.mu.RLock()
	job, ok := s.jobs[id]
	if !ok {
		s.mu.RUnlock()
		return
	}
	payload := job.Payload
	s.mu.RUnlock()

	s.mu.Lock()
	if current, ok := s.jobs[id]; ok {
		current.Status = StatusProcessing
		current.Error = ""
	}
	s.mu.Unlock()

	err := s.processor.Process(context.Background(), payload)

	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.jobs[id]; ok {
		if err != nil {
			current.Status = StatusFailed
			current.Error = err.Error()
			return
		}
		current.Status = StatusCompleted
		current.Error = ""
	}
}

func (s *Service) Stop() {
	s.stopOnce.Do(func() {
		s.stopping.Store(true)
		close(s.queue)
	})
	s.wg.Wait()
}

func cloneJob(j *Job) *Job {
	copy := *j
	return &copy
}
