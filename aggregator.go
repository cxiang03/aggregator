package aggregator

import (
	"sync"
	"time"
)

type Aggregator[T any, U any, V any] struct {
	DebugMode bool
	closeCh   chan struct{}
	closedCh  chan struct{}

	WorkerCount   int
	wg            *sync.WaitGroup
	workers       []*worker[T, U, V]
	TaskCh        chan<- T
	taskCh        chan T
	BatchSize     int
	BatchInterval time.Duration

	NewSum    func() U
	Reduce    func(U, T) U
	BeforeAct func(U) error
	Action    func(U) (V, error)
	AfterAct  func(U, V, error)
}

func (a *Aggregator[T, U, V]) Start() error {
	a.closeCh = make(chan struct{})
	a.closedCh = make(chan struct{})

	a.taskCh = make(chan T)
	a.TaskCh = a.taskCh

	a.wg = &sync.WaitGroup{}
	a.workers = make([]*worker[T, U, V], 0, a.WorkerCount)
	for i := 0; i < a.WorkerCount; i++ {
		w := &worker[T, U, V]{
			parent: a,
		}
		a.workers = append(a.workers, w)
		go w.run()
	}
	a.wg.Wait()
	a.closedCh <- struct{}{}
	return nil
}

func (a *Aggregator[T, U, V]) Stop() error {
	close(a.closeCh)
	<-a.closedCh
	return nil
}
