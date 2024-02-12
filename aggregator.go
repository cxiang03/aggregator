package aggregator

import (
	"sync"
	"time"
)

// Aggregator - the entry point as a aggregator group.
//   - T - tasks type that are going to be processed by this worker.
//   - U - type of the intermediate result from the aggregation of tasks (the sum).
//   - V - final result type after performing some action on the sum.
//   - DebugMode - debug or not. # TODO - implement debug mode.
//   - WorkerCount - workers count to create, each worker can be regard as a group to sum up all collected tasks.
//   - BatchSize - number of tasks to be collected before performing the action.
//   - BatchInterval - time interval to perform the action, if the number of tasks collected is less than BatchSize.
//   - NewSum - function to create a new sum object (U) when worker is reset after performing the action.
//   - Reduce - function to reduce the sum object (U) after getting a new task (T) with existing sum object (U).
//   - BeforeAct - function to be called before performing the action, like validation, optional.
//   - Action - function to perform the action on the sum object (U) and return the result (V).
//   - AfterAct - function to be called after performing the action, like error tracking, optional.
type Aggregator[T any, U any, V any] struct {
	DebugMode bool
	closeCh   chan struct{}
	closedCh  chan struct{}

	WorkerCount   int
	wg            *sync.WaitGroup
	workers       []*worker[T, U, V]
	TaskCh        chan T
	BatchSize     int
	BatchInterval time.Duration

	NewSum    func() U
	Reduce    func(U, T) U
	BeforeAct func(U) error
	Action    func(U) (V, error)
	AfterAct  func(U, V, error)
}

// Start - initialize the aggregator and start its workers.
func (a *Aggregator[T, U, V]) Start() error {
	a.closeCh = make(chan struct{})
	a.closedCh = make(chan struct{})
	a.TaskCh = make(chan T)

	a.wg = &sync.WaitGroup{}
	a.workers = make([]*worker[T, U, V], 0, a.WorkerCount)
	for i := 0; i < a.WorkerCount; i++ {
		w := &worker[T, U, V]{
			sum:    a.NewSum(),
			parent: a,
		}
		a.workers = append(a.workers, w)
		go w.run()
	}
	a.wg.Wait()
	a.closedCh <- struct{}{}
	return nil
}

// Stop - stop the aggregator and its workers.
func (a *Aggregator[T, U, V]) Stop() error {
	close(a.closeCh)
	<-a.closedCh
	return nil
}
