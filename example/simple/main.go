package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cxiang03/aggregator"
)

func main() {
	agg := &aggregator.Aggregator[int, []int, int]{
		WorkerCount:   5,
		BatchSize:     50,
		BatchInterval: 5 * time.Millisecond,
		NewSum: func() []int {
			return []int{}
		},
		Reduce: func(sum []int, task int) []int {
			return append(sum, task)
		},
		BeforeAct: func(sum []int) error {
			log.Println("before act sum is", sum)
			return nil
		},
		Action: func(sum []int) (int, error) {
			rst := 0
			for _, v := range sum {
				rst += v
			}
			return rst, nil
		},
		AfterAct: func(sum []int, rst int, err error) {
			fmt.Println("group result is", sum, rst, err)
		},
	}

	go func() {
		_ = agg.Start()
	}()

	wg := &sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				time.Sleep(10 * time.Millisecond)
				agg.TaskCh <- i + j
			}
		}()
	}

	wg.Wait()
	_ = agg.Stop()
}
