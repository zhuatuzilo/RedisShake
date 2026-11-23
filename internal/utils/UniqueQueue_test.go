package utils

import (
	"sync"
	"testing"
	"time"
)

func TestUniqueQueueBackpressureAndClose(t *testing.T) {
	q := NewUniqueQueue[int](1)
	if !q.Put(1) || !q.Put(1) || q.Len() != 1 || q.Cap() != 1 {
		t.Fatal("duplicate should succeed without taking space")
	}
	put := make(chan bool, 1)
	go func() { put <- q.Put(2) }()
	select {
	case <-put:
		t.Fatal("distinct item entered a full queue")
	case <-time.After(20 * time.Millisecond):
	}
	if item, ok := q.Get(); !ok || item != 1 {
		t.Fatalf("first get = %d, %v", item, ok)
	}
	select {
	case accepted := <-put:
		if !accepted {
			t.Fatal("put rejected")
		}
	case <-time.After(time.Second):
		t.Fatal("producer stayed blocked")
	}
	go func() { put <- q.Put(3) }()
	q.Close()
	q.Close()
	select {
	case accepted := <-put:
		if accepted {
			t.Fatal("put accepted by closed queue")
		}
	case <-time.After(time.Second):
		t.Fatal("close did not release producer")
	}
	if item, ok := q.Get(); !ok || item != 2 {
		t.Fatalf("drain = %d, %v", item, ok)
	}
	if _, ok := q.Get(); ok {
		t.Fatal("closed queue still has items")
	}
}

func TestUniqueQueueConcurrentProducers(t *testing.T) {
	const producers, items = 4, 100
	q := NewUniqueQueue[int](7)
	var wg sync.WaitGroup
	for producer := range producers {
		wg.Go(func() {
			for i := range items {
				q.Put(producer*items + i)
			}
		})
	}
	go func() { wg.Wait(); q.Close() }()
	seen := make(map[int]bool)
	for item, ok := q.Get(); ok; item, ok = q.Get() {
		if seen[item] {
			t.Fatalf("repeated %d", item)
		}
		seen[item] = true
	}
	if len(seen) != producers*items {
		t.Fatalf("consumed %d items", len(seen))
	}
}
