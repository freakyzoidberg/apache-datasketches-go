package frequencies

import (
	"fmt"
	"github.com/apache/datasketches-go/internal"
)

type reversePurgeItemHashMap[C any] struct {
	lgLength      int
	loadThreshold int
	keys          []C
	values        []int64
	states        []int16
	numActive     int
}

// newReversePurgeItemHashMap constructs a new reversePurgeItemHashMap.
// It will create arrays of length mapSize, which must be a power of two.
// This restriction was made to ensure fast hashing.
// The member loadThreshold is then set to the largest value that
// will not overload the hash table.
func newReversePurgeItemHashMap[C any](mapSize int) (*reversePurgeItemHashMap[C], error) {
	lgLength, err := internal.ExactLog2(mapSize)
	if err != nil {
		return nil, fmt.Errorf("mapSize: %e", err)
	}
	loadThreshold := int(float64(mapSize) * reversePurgeLongHashMapLoadFactor)
	keys := make([]C, mapSize)
	values := make([]int64, mapSize)
	states := make([]int16, mapSize)
	return &reversePurgeItemHashMap[C]{
		lgLength:      lgLength,
		loadThreshold: loadThreshold,
		keys:          keys,
		values:        values,
		states:        states,
	}, nil
}

// getCapacity returns the current capacity of the hash map (i.e., max number of keys that can be stored).
func (r *reversePurgeItemHashMap[C]) getCapacity() int {
	return r.loadThreshold
}
