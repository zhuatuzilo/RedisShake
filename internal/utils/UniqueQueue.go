package utils

import "sync"

// UniqueQueue is a bounded FIFO that coalesces equal items while they are queued.
type UniqueQueue[T comparable] struct {
	mu                   sync.Mutex
	readable, writable   *sync.Cond
	items                []T
	pending              map[T]struct{}
	head, size, capacity int
	closed               bool
}

func NewUniqueQueue[T comparable](capacity int) *UniqueQueue[T] {
	if capacity <= 0 {
		panic("unique queue capacity must be positive")
	}
	q := &UniqueQueue[T]{capacity: capacity, pending: make(map[T]struct{})}
	q.readable = sync.NewCond(&q.mu)
	q.writable = sync.NewCond(&q.mu)
	return q
}

// Put waits for space if item is not already queued. It returns false on closure.
func (q *UniqueQueue[T]) Put(item T) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for !q.closed {
		if _, exists := q.pending[item]; exists {
			return true
		}
		if q.size < q.capacity {
			if q.size == len(q.items) {
				items := make([]T, min(q.capacity, max(1, 2*len(q.items))))
				n := copy(items, q.items[q.head:])
				copy(items[n:], q.items[:q.head])
				q.items, q.head = items, 0
			}
			q.items[(q.head+q.size)%len(q.items)] = item
			q.size++
			q.pending[item] = struct{}{}
			q.readable.Signal()
			return true
		}
		q.writable.Wait()
	}
	return false
}

// Get waits for an item, or returns false once a closed queue has drained.
func (q *UniqueQueue[T]) Get() (item T, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.size == 0 && !q.closed {
		q.readable.Wait()
	}
	if q.size == 0 {
		return item, false
	}
	item = q.items[q.head]
	var zero T
	q.items[q.head] = zero
	q.head = (q.head + 1) % len(q.items)
	q.size--
	delete(q.pending, item)
	q.writable.Broadcast()
	return item, true
}

func (q *UniqueQueue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.size
}

func (q *UniqueQueue[T]) Cap() int { return q.capacity }

// Close stops producers and lets consumers drain accepted items.
func (q *UniqueQueue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.readable.Broadcast()
	q.writable.Broadcast()
}
