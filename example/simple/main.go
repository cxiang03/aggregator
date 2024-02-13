package main

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/cxiang03/aggregator"
)

func main() {
	// create a new aggregator for group a bunch of int tasks and sum them up into a string. with:
	// * 5 workers
	// * do sum up when worker get 20 numbers(task)
	// * do sum up every 100 milliseconds if worker get less than 20 numbers(task)
	// * reset - prepare a new []int to collect new numbers
	// * reduce - append collected number (task) to sum object ([]int)
	// * action - sum up all numbers in the sum object ([]int) and return a string
	// * before action - print out the sum object ([]int)
	// * after action - print out the sum object ([]int), result (string) and error (error)
	agg := &aggregator.Aggregator[int, []int, string]{
		WorkerCount:   5,
		BatchSize:     20,
		BatchInterval: 100 * time.Millisecond,
		NewSum: func() []int {
			return []int{}
		},
		Reduce: func(sum []int, task int) []int {
			return append(sum, task)
		},
		BeforeAct: func(sum []int) error {
			log.Println("before act - len is", len(sum))
			return nil
		},
		Action: func(sum []int) (string, error) {
			rst := 0
			for _, v := range sum {
				rst += v
			}
			return strconv.Itoa(rst), nil
		},
		AfterAct: func(sum []int, rst string, err error) {
			fmt.Println("group result is", sum, rst, err)
		},
	}

	// start the aggregator
	go func() {
		_ = agg.Start()
	}()

	// send 10000 tasks to the aggregator by 100 goroutines concurrently
	wg := &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
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
