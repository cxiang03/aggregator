package main

import (
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/cxiang03/aggregator"
)

type Item struct {
	Key string
	Val string
}

// mset mock redis MSET command.
func mset(_ ...string) error {
	return nil
}

func main() {
	// redisClient mock a redis client pool.
	var redisClient = make(chan int)
	go func() {
		redisClient <- 1
	}()

	// create a new aggregator for group a bunch of redis set items and sum them up into a single mset cmd. with:
	// * 1 worker only
	// * do sum up when worker get 10 items
	// * do sum up every 10 milliseconds if worker get less than 10 numbers
	// * reset - prepare a new map[string]string to collect new items
	// * reduce - append/override collected item (task) to sum object (map[string]string)
	// * action - async call redis mset all items in the sum object (map[string]string) and return the number of items
	// * before action - nil
	// * after action - print some log
	agg := &aggregator.Aggregator[*Item, map[string]string, int]{
		WorkerCount:   1,
		BatchSize:     10,
		BatchInterval: 10 * time.Millisecond,
		NewSum: func() map[string]string {
			return map[string]string{}
		},
		Reduce: func(sum map[string]string, item *Item) map[string]string {
			sum[item.Key] = item.Val
			return sum
		},
		BeforeAct: nil,
		Action: func(sum map[string]string) (int, error) {
			<-redisClient
			kv := make([]string, 0, len(sum)*2)
			for k, v := range sum {
				kv = append(kv, k, v)
			}

			// make redis MSET async to avoid blocking the aggregator
			// and apply rate limit the redis client pool to avoid too many goroutines
			go func() {
				defer func() {
					redisClient <- 1
				}()

				if err := mset(kv...); err != nil {
					log.Println("mset error", err)
				}
			}()
			return len(sum), nil
		},
		AfterAct: func(m map[string]string, i int, err error) {
			log.Println("about to mset len is", len(m), "result is", i, "error is", err)
		},
	}

	// start the aggregator
	go func() {
		_ = agg.Start()
	}()

	// send 10000 tasks to the aggregator by 100 goroutines concurrently
	wg := &sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				time.Sleep(2 * time.Millisecond)
				agg.TaskCh <- &Item{
					Key: strconv.Itoa(i) + strconv.Itoa(j),
					Val: strconv.Itoa(i) + strconv.Itoa(j),
				}
			}
		}()
	}
	wg.Wait()
	_ = agg.Stop()
	// aha! we can close the redis client pool now
	<-redisClient
}
