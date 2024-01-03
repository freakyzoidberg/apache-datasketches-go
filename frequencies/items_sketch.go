/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package frequencies

import (
	"encoding/binary"
	"fmt"
	"github.com/apache/datasketches-go/internal"
	"sort"
)

type ItemsSketch[C comparable] struct {
	// Log2 Maximum length of the arrays internal to the hashFn map supported by the data
	// structure.
	lgMaxMapSize int
	// The current number of counters supported by the hashFn map.
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

type ItemSketchOp[C comparable] interface {
	Hash(item C) uint64
	SerializeOneToSlice(item C) []byte
	SerializeManyToSlice(item []C) []byte
	DeserializeManyFromSlice(slc []byte, offset int, length int) []C
}

// NewItemsSketch constructs a new ItemsSketch with the given parameters.
// this internal constructor is used when deserializing the sketch.
//
//   - lgMaxMapSize, log2 of the physical size of the internal hashFn map managed by this
//     sketch. The maximum capacity of this internal hashFn map is 0.75 times 2^lgMaxMapSize.
//     Both the ultimate accuracy and size of this sketch are functions of lgMaxMapSize.
//   - lgCurMapSize, log2 of the starting (current) physical size of the internal hashFn
//     map managed by this sketch.
func NewItemsSketch[C comparable](lgMaxMapSize int, lgCurMapSize int, operations ItemSketchOp[C]) (*ItemsSketch[C], error) {
	lgMaxMapSz := max(lgMaxMapSize, _LG_MIN_MAP_SIZE)
	lgCurMapSz := max(lgCurMapSize, _LG_MIN_MAP_SIZE)
	hashMap, err := newReversePurgeItemHashMap[C](1<<lgCurMapSz, operations)
	if err != nil {
		return nil, err
	}
	curMapCap := hashMap.getCapacity()
	maxMapCap := int(float64(uint64(1)<<lgMaxMapSize) * reversePurgeItemHashMapLoadFactor)
	offset := int64(0)
	sampleSize := min(_SAMPLE_SIZE, maxMapCap)

	return &ItemsSketch[C]{
		lgMaxMapSize: lgMaxMapSz,
		curMapCap:    curMapCap,
		offset:       offset,
		sampleSize:   sampleSize,
		hashMap:      hashMap,
	}, nil
}

// NewItemsSketchWithMaxMapSize constructs a new ItemsSketch with the given maxMapSize and the default
// initialMapSize (8).
//
//   - maxMapSize, Determines the physical size of the internal hashFn map managed by this
//     sketch and must be a power of 2. The maximum capacity of this internal hashFn map is
//     0.75 times * maxMapSize. Both the ultimate accuracy and size of this sketch are
//     functions of maxMapSize.
func NewItemsSketchWithMaxMapSize[C comparable](maxMapSize int, operations ItemSketchOp[C]) (*ItemsSketch[C], error) {
	maxMapSz, err := internal.ExactLog2(maxMapSize)
	if err != nil {
		return nil, err
	}
	return NewItemsSketch[C](maxMapSz, _LG_MIN_MAP_SIZE, operations)
}

func NewItemsSketchFromSlice[C comparable](slc []byte, operations ItemSketchOp[C]) (*ItemsSketch[C], error) {
	pre0, err := checkPreambleSize(slc) //make sure preamble will fit
	maxPreLongs := internal.FamilyEnum.Frequency.MaxPreLongs

	preLongs := extractPreLongs(pre0)                     //Byte 0
	serVer := extractSerVer(pre0)                         //Byte 1
	familyID := extractFamilyID(pre0)                     //Byte 2
	lgMaxMapSize := extractLgMaxMapSize(pre0)             //Byte 3
	lgCurMapSize := extractLgCurMapSize(pre0)             //Byte 4
	empty := (extractFlags(pre0) & _EMPTY_FLAG_MASK) != 0 //Byte 5

	// Checks
	preLongsEq1 := (preLongs == 1) //Byte 0
	preLongsEqMax := (preLongs == maxPreLongs)
	if !preLongsEq1 && !preLongsEqMax {
		return nil, fmt.Errorf("possible corruption: preLongs must be 1 or %d: %d", maxPreLongs, preLongs)
	}
	if serVer != _SER_VER { //Byte 1
		return nil, fmt.Errorf("possible corruption: ser ver must be %d: %d", _SER_VER, serVer)
	}
	actFamID := internal.FamilyEnum.Frequency.Id //Byte 2
	if familyID != actFamID {
		return nil, fmt.Errorf("possible corruption: familyID must be %d: %d", actFamID, familyID)
	}
	if empty && !preLongsEq1 { //Byte 5 and Byte 0
		return nil, fmt.Errorf("(preLongs == 1) ^ empty == true")
	}
	if empty {
		return NewItemsSketchWithMaxMapSize[C](1<<_LG_MIN_MAP_SIZE, operations)
	}
	// Get full preamble
	preArr := make([]int64, preLongs)
	for j := 0; j < preLongs; j++ {
		preArr[j] = int64(binary.LittleEndian.Uint64(slc[j<<3:]))
	}

	fis, err := NewItemsSketch[C](int(lgMaxMapSize), int(lgCurMapSize), operations)
	if err != nil {
		return nil, err
	}
	fis.streamWeight = 0 // update after
	fis.offset = preArr[3]

	preBytes := preLongs << 3
	activeItems := extractActiveItems(preArr[1])

	// Get countArray
	countArray := make([]int64, activeItems)
	reqBytes := preBytes + activeItems*8 // count Arr only
	if len(slc) < reqBytes {
		return nil, fmt.Errorf("possible Corruption: Insufficient bytes in array: %d, %d", len(slc), reqBytes)
	}
	for j := 0; j < activeItems; j++ {
		countArray[j] = int64(binary.LittleEndian.Uint64(slc[preBytes+j<<3:]))
	}
	// Get itemArray
	itemsOffset := preBytes + (8 * activeItems)
	itemArray := operations.DeserializeManyFromSlice(slc[itemsOffset:], 0, activeItems)
	// update the sketch
	for j := 0; j < activeItems; j++ {
		err := fis.UpdateMany(itemArray[j], int(countArray[j]))
		if err != nil {
			return nil, err
		}
	}
	fis.streamWeight = preArr[2] // override streamWeight due to updating
	return fis, nil
}

func (i *ItemsSketch[C]) Reset() error {
	hashMap, err := newReversePurgeItemHashMap[C](1<<_LG_MIN_MAP_SIZE, i.hashMap.operations)
	if err != nil {
		return err
	}
	i.hashMap = hashMap
	i.curMapCap = hashMap.getCapacity()
	i.offset = 0
	i.streamWeight = 0
	return nil
}

// IsEmpty returns true if this sketch is empty.
func (i *ItemsSketch[C]) IsEmpty() bool {
	return i.GetNumActiveItems() == 0
}

// GetNumActiveItems returns the number of active items in the sketch.
func (i *ItemsSketch[C]) GetNumActiveItems() int {
	return i.hashMap.numActive
}

// GetStreamLength returns the sum of the frequencies in the stream seen so far by the sketch.
func (i *ItemsSketch[C]) GetStreamLength() int64 {
	return i.streamWeight
}

func (i *ItemsSketch[C]) Update(item C) error {
	return i.UpdateMany(item, 1)
}

func (i *ItemsSketch[C]) UpdateMany(item C, count int) error {
	if isNil(item) || count == 0 {
		return nil
	}
	if count < 0 {
		return fmt.Errorf("count may not be negative")
	}

	i.streamWeight += int64(count)
	err := i.hashMap.adjustOrPutValue(item, int64(count))
	if err != nil {
		return err
	}

	if i.GetNumActiveItems() > i.curMapCap { //over the threshold, we need to do something
		if i.hashMap.lgLength < i.lgMaxMapSize { //below tgt size, we can grow
			err := i.hashMap.resize(2 * len(i.hashMap.keys))
			if err != nil {
				return err
			}
			i.curMapCap = i.hashMap.getCapacity()
		} else {
			i.offset += i.hashMap.purge(i.sampleSize)
			if i.GetNumActiveItems() > i.hashMap.getCapacity() {
				return fmt.Errorf("purge did not reduce active items")
			}
		}
	}
	return nil
}

func (i *ItemsSketch[C]) GetEstimate(item C) (int64, error) {
	// If item is tracked:
	// Estimate = itemCount + offset; Otherwise it is 0.
	v, err := i.hashMap.get(item)
	if v > 0 {
		return v + i.offset, err
	}
	return 0, err
}

// GetLowerBound gets the guaranteed lower bound frequency of the given item, which can never be
// negative.
//
//   - item, the given item.
func (i *ItemsSketch[C]) GetLowerBound(item C) (int64, error) {
	return i.hashMap.get(item)
}

// GetUpperBound gets the guaranteed upper bound frequency of the given item.
//
//   - item, the given item.
func (i *ItemsSketch[C]) GetUpperBound(item C) (int64, error) {
	// UB = itemCount + offset
	v, err := i.hashMap.get(item)
	return v + i.offset, err
}

func (i *ItemsSketch[C]) GetMaximumError() int64 {
	return i.offset
}

func (i *ItemsSketch[C]) GetFrequentItems(errorType errorType) ([]*RowItem[C], error) {
	return i.sortItems(i.GetMaximumError(), errorType)
}

func (i *ItemsSketch[C]) GetFrequentItemsWithThreshold(threshold int64, errorType errorType) ([]*RowItem[C], error) {
	finalThreshold := i.GetMaximumError()
	if threshold > finalThreshold {
		finalThreshold = threshold
	}
	return i.sortItems(finalThreshold, errorType)
}

func (i *ItemsSketch[C]) ToSlice() []byte {
	preLongs := 0
	outBytes := 0
	empty := i.IsEmpty()
	activeItems := i.GetNumActiveItems()
	bytes := make([]byte, 0)
	if empty {
		preLongs = 1
		outBytes = 8
	} else {
		preLongs = internal.FamilyEnum.Frequency.MaxPreLongs
		bytes = i.hashMap.operations.SerializeManyToSlice(i.hashMap.getActiveKeys())
		outBytes = ((preLongs + activeItems) << 3) + len(bytes)
	}

	outArr := make([]byte, outBytes)
	pre0 := int64(0)
	pre0 = insertPreLongs(int64(preLongs), pre0)                         //Byte 0
	pre0 = insertSerVer(_SER_VER, pre0)                                  //Byte 1
	pre0 = insertFamilyID(int64(internal.FamilyEnum.Frequency.Id), pre0) //Byte 2
	pre0 = insertLgMaxMapSize(int64(i.lgMaxMapSize), pre0)               //Byte 3
	pre0 = insertLgCurMapSize(int64(i.hashMap.lgLength), pre0)           //Byte 4
	if empty {
		pre0 = insertFlags(_EMPTY_FLAG_MASK, pre0) //Byte 5
	} else {
		pre0 = insertFlags(0, pre0) //Byte 5
	}

	if empty {
		binary.LittleEndian.PutUint64(outArr, uint64(pre0))
	} else {
		pre := int64(0)
		preArr := make([]int64, preLongs)
		preArr[0] = pre0
		preArr[1] = insertActiveItems(int64(activeItems), pre)
		preArr[2] = int64(i.streamWeight)
		preArr[3] = int64(i.offset)
		for j := 0; j < preLongs; j++ {
			binary.LittleEndian.PutUint64(outArr[j<<3:], uint64(preArr[j]))
		}
		preBytes := preLongs << 3
		for j := 0; j < activeItems; j++ {
			binary.LittleEndian.PutUint64(outArr[preBytes+j<<3:], uint64(i.hashMap.getActiveValues()[j]))
		}
		copy(outArr[preBytes+(activeItems<<3):], bytes)
	}
	return outArr
}

func (i *ItemsSketch[C]) sortItems(threshold int64, errorType errorType) ([]*RowItem[C], error) {
	rowList := make([]*RowItem[C], 0)
	iter := i.hashMap.iterator()
	if errorType == ErrorTypeEnum.NoFalseNegatives {
		for iter.next() {
			est, err := i.GetEstimate(iter.getKey())
			if err != nil {
				return nil, err
			}
			ub, err := i.GetUpperBound(iter.getKey())
			if err != nil {
				return nil, err
			}
			lb, err := i.GetLowerBound(iter.getKey())
			if err != nil {
				return nil, err
			}
			if ub >= threshold {
				row := newRowItem[C](iter.getKey(), est, ub, lb)
				rowList = append(rowList, row)
			}
		}
	} else { //NO_FALSE_POSITIVES
		for iter.next() {
			est, err := i.GetEstimate(iter.getKey())
			if err != nil {
				return nil, err
			}
			ub, err := i.GetUpperBound(iter.getKey())
			if err != nil {
				return nil, err
			}
			lb, err := i.GetLowerBound(iter.getKey())
			if err != nil {
				return nil, err
			}
			if lb >= threshold {
				row := newRowItem[C](iter.getKey(), est, ub, lb)
				rowList = append(rowList, row)
			}
		}
	}

	sort.Slice(rowList, func(i, j int) bool {
		return rowList[i].est > rowList[j].est
	})

	return rowList, nil
}
