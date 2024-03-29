package aggregator

import (
	"time"
)

// worker is a practical processor in the aggregator logic.
// It handles aggregating tasks in batches either based on a time interval by a timer or a batch size.
//   - T - tasks type that are going to be processed by this worker.
//   - U - type of the intermediate result from the aggregation of tasks (the sum).
//   - V - final result type after performing some action on the sum.
type worker[T any, U any, V any] struct {
	timer  *time.Timer
	count  int
	sum    U
	parent *Aggregator[T, U, V]
}

// run - main loop when the worker starts.
func (w *worker[T, U, V]) run() {
	w.parent.wg.Add(1)
	defer w.parent.wg.Done()

	w.timer = time.NewTimer(w.parent.BatchInterval)
	defer w.timer.Stop()

	for {
		select {
		case <-w.parent.closeCh:
			w.flush()
			w.reset(true)
			return
		case task := <-w.parent.TaskCh:
			w.count++
			w.sum = w.parent.Reduce(w.sum, task)
			if w.count >= w.parent.BatchSize {
				w.flush()
				w.reset(true)
			}
		case <-w.timer.C:
			w.flush()
			w.reset(false)
		}
	}
}

// flush - executes the action on the sum.
func (w *worker[T, U, V]) flush() {
	if w.parent.BeforeAct != nil {
		if err := w.parent.BeforeAct(w.sum); err != nil {
			return
		}
	}

	rst, err := w.parent.Action(w.sum)

	if w.parent.AfterAct != nil {
		w.parent.AfterAct(w.sum, rst, err)
	}
}

// reset - resets the worker state.
func (w *worker[T, U, V]) reset(stopTimer bool) {
	// stop the timer if it's not already stopped and reset it.
	// for zombie timer issue, we need to drain the timer channel to avoid a leak.
	// please refer timer.Reset() comments for details.
	if stopTimer && !w.timer.Stop() {
		<-w.timer.C
	}
	w.timer.Reset(w.parent.BatchInterval)
	w.count = 0
	w.sum = w.parent.NewSum()
}
