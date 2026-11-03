package util

// Set is a collection type that provides methods for adding, removing and checking the existence of items.
type Set[T comparable] struct {
	m map[T]bool
}

// NewSet returns a pointer to a new Set object
func NewSet[T comparable]() *Set[T] {
	return &Set[T]{
		m: make(map[T]bool),
	}
}

// Add tries to add the provided item to the set.
// Returns true if the item was added, or false if the item already exists in the collection.
func (s *Set[T]) Add(item T) bool {
	if _, ok := s.m[item]; ok {
		return false
	} else {
		s.m[item] = true
		return true
	}
}

// Remove removes the provided item from the set.
// Returns true if the item was removed, or false if the item wasn't in the set.
func (s *Set[T]) Remove(item T) bool {
	if _, ok := s.m[item]; !ok {
		return false
	} else {
		delete(s.m, item)
		return true
	}
}

// Contains tests the existence of the provided item in the set.
// Returns true if the item is in the set, otherwise false.
func (s *Set[T]) Contains(item T) bool {
	_, ok := s.m[item]
	return ok
}
