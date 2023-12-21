package frequencies

import (
	"fmt"
	"github.com/apache/datasketches-go/internal"
)

type ItemsSketch[C any] struct {
	// Log2 Maximum length of the arrays internal to the hash map supported by the data
	// structure.
	lgMaxMapSize int
	// The current number of counters supported by the hash map.
	curMapCap int //the threshold to purge
	// Tracks the total of decremented counts.
	offset int64
	// The sum of all frequencies of the stream so far.
	streamWeight int64
	// The maximum number of samples used to compute approximate median of counters when doing
	// decrement
	sampleSize int
	// Hash map mapping stored items to approximate counts
	hashMap *reversePurgeItemHashMap[C]
}

// NewItemsSketch returns a new LongsSketch with the given lgMaxMapSize and lgCurMapSize.
//
// lgMaxMapSize is the log2 of the physical size of the internal hash map managed by this
// sketch. The maximum capacity of this internal hash map is 0.75 times 2^lgMaxMapSize.
// Both the ultimate accuracy and size of this sketch are a function of lgMaxMapSize.
//
// lgCurMapSize is the log2 of the starting (current) physical size of the internal hash
// map managed by this sketch.
func NewItemsSketch[C any](lgMaxMapSize int, lgCurMapSize int) (*ItemsSketch[C], error) {
	//set initial size of hash map
	lgMaxMapSize = max(lgMaxMapSize, _LG_MIN_MAP_SIZE)
	lgCurMapSize = max(lgCurMapSize, _LG_MIN_MAP_SIZE)

	hashMap, err := newReversePurgeItemHashMap[C](1 << lgCurMapSize)
	if err != nil {
		return nil, err
	}
	curMapCap := hashMap.getCapacity()
	maxMapCap := int(float64(uint64(1<<lgMaxMapSize)) * reversePurgeLongHashMapLoadFactor)
	offset := int64(0)
	sampleSize := min(_SAMPLE_SIZE, maxMapCap)
	return &ItemsSketch[C]{
		lgMaxMapSize: lgMaxMapSize,
		curMapCap:    curMapCap,
		offset:       offset,
		sampleSize:   sampleSize,
		hashMap:      hashMap,
	}, nil
}

// NewItemsSketchWithMaxMapSize constructs a new ItemsSketch with the given maxMapSize and the
// default initialMapSize (8).
//
// maxMapSize determines the physical size of the internal hash map managed by this
// sketch and must be a power of 2.  The maximum capacity of this internal hash map is
// 0.75 times * maxMapSize. Both the ultimate accuracy and size of this sketch are a
// function of maxMapSize.
func NewItemsSketchWithMaxMapSize[C any](maxMapSize int) (*ItemsSketch[C], error) {
	log2OfInt, err := internal.ExactLog2(maxMapSize)
	if err != nil {
		return nil, fmt.Errorf("maxMapSize, %e", err)
	}
	return NewItemsSketch[C](log2OfInt, _LG_MIN_MAP_SIZE)
}
