// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================

package mlq

import (
	"math/bits"
	"sync"
)

// PriorityQueue represents a single level of priority in the MLQ.
type PriorityQueue[T any] struct {
	Items []T
}

// MultiLevelQueue is a high-performance, thread-safe, multi-priority queue.
// It uses a bitmask for O(1) priority selection and hardware acceleration.
type MultiLevelQueue[T any] struct {
	mu sync.RWMutex

	queues  [6]PriorityQueue[T]
	bitmask uint16 // Bit i is 1 if queues[i] is not empty

	// Global census for O(1) existence and duplicate prevention across all levels
	census map[uint32]T
}

// New creates a new MultiLevelQueue with an initial census capacity.
func New[T any](initialCapacity int) *MultiLevelQueue[T] {
	m := &MultiLevelQueue[T]{
		census: make(map[uint32]T, initialCapacity),
	}
	for i := 0; i < 6; i++ {
		m.queues[i].Items = make([]T, 0, 16)
	}
	return m
}

// Push adds an item to the queue at the specified priority if the key is unique.
func (m *MultiLevelQueue[T]) Push(priority int, key uint32, item T) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Duplicate check (O(1))
	if _, exists := m.census[key]; exists {
		return false
	}

	if priority < 0 || priority >= 6 {
		priority = 3 // Default
	}

	// 2. Add to queue
	q := &m.queues[priority]
	q.Items = append(q.Items, item)

	// 3. Update census and bitmask
	m.census[key] = item
	m.bitmask |= (1 << uint(priority))

	return true
}

// Pop retrieves the highest priority item from the queue.
func (m *MultiLevelQueue[T]) Pop(keyExtractor func(T) uint32) (T, int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.popLocked(keyExtractor)
}

func (m *MultiLevelQueue[T]) popLocked(keyExtractor func(T) uint32) (T, int, bool) {
	if m.bitmask == 0 {
		var zero T
		return zero, 0, false
	}

	// Optimized: Use hardware instruction to find highest priority (trailing zeros)
	priority := bits.TrailingZeros16(m.bitmask)

	q := &m.queues[priority]
	if len(q.Items) == 0 {
		m.bitmask &= ^(1 << uint(priority))
		return m.popLocked(keyExtractor)
	}

	item := q.Items[0]

	// Memory safety: Clear the pointer from the slice to avoid leaks if T is a pointer
	var zero T
	q.Items[0] = zero
	q.Items = q.Items[1:]

	// Update census and bitmask
	if keyExtractor != nil {
		delete(m.census, keyExtractor(item))
	}

	if len(q.Items) == 0 {
		m.bitmask &= ^(1 << uint(priority))
	}

	return item, priority, true
}

// Get checks if an item exists in the queue using its tracking key.
func (m *MultiLevelQueue[T]) Get(key uint32) (T, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	item, exists := m.census[key]
	return item, exists
}

// Count returns the number of items in a specific priority queue.
func (m *MultiLevelQueue[T]) Count(priority int) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if priority < 0 || priority >= 6 {
		return 0
	}
	return len(m.queues[priority].Items)
}

// Size returns the total number of items in all queues.
func (m *MultiLevelQueue[T]) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.census)
}

// Clear empties all queues and reset the bitmask.
// If a callback is provided, it is invoked for each item before clearing.
func (m *MultiLevelQueue[T]) Clear(callback func(T)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.queues {
		if callback != nil {
			for _, item := range m.queues[i].Items {
				callback(item)
			}
		}
		// Clear slice and free memory
		m.queues[i].Items = nil
	}
	m.census = make(map[uint32]T)
	m.bitmask = 0
}
