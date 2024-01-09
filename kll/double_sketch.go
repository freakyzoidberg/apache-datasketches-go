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

package kll

import (
	"errors"
	"math"
	"math/rand"
)

type DoubleSketch struct {
	sketchType
	sketchStructure
	readOnly  bool
	levelsArr []int //Always writable form

	k                 int   // configured size of K.
	m                 int   // configured size of M.
	n                 int64 // number of items input into this sketch.
	minK              int   // dynamic minK for error estimation after merging with different k.
	isLevelZeroSorted bool
	minDoubleItem     float64
	maxDoubleItem     float64
	doubleItems       []float64
}

// NewKllDoubleSketch return a new DoubleSketch with a given parameters k and m.
//
// k parameter that controls size of the sketch and accuracy of estimates.
// k can be between m and 65535, inclusive.
// The default k = 200 results in a normalized rank error of about 1.65%.
// Larger k will have smaller error but the sketch will be larger (and slower).
//
// m parameter controls the minimum level width in items. It can be 2, 4, 6 or 8.
// The DEFAULT_M, which is 8 is recommended. Other sizes of m should be considered
// experimental as they have not been as well characterized
func NewKllDoubleSketch(k int, m int) (*DoubleSketch, error) {
	if err := checkK(k, m); err != nil {
		return nil, err
	}
	if err := checkM(m); err != nil {
		return nil, err

	}

	return &DoubleSketch{
		sketchType:        sketchTypeEnum.doubleSketch,
		sketchStructure:   sketchStructureEnum.updatable,
		readOnly:          false,
		levelsArr:         []int{k, k},
		k:                 k,
		m:                 m,
		n:                 0,
		minK:              k,
		isLevelZeroSorted: false,
		minDoubleItem:     math.NaN(),
		maxDoubleItem:     math.NaN(),
		doubleItems:       make([]float64, k),
	}, nil
}

func NewKllDoubleSketchWithDefault() *DoubleSketch {
	sketch, _ := NewKllDoubleSketch(_DEFAULT_K, _DEFAULT_M)
	return sketch
}

func (k *DoubleSketch) Update(value float64) error {
	if k.readOnly {
		return errors.New("Target sketch is Read Only, cannot write. ")
	}
	err := updateDouble(k, value)
	// skval

	return err
}

func (k *DoubleSketch) GetNumRetained() int {
	return k.levelsArr[k.getNumLevels()] - k.levelsArr[0]
}

func (k *DoubleSketch) GetN() int64 {
	return k.n
}

func (k *DoubleSketch) IsEmpty() bool {
	return k.GetN() == 0
}

func (k *DoubleSketch) setMinItem(item float64) {
	k.minDoubleItem = item
}

func (k *DoubleSketch) setMaxItem(item float64) {
	k.maxDoubleItem = item
}

func (k *DoubleSketch) getMinItem() float64 {
	return k.minDoubleItem
}

func (k *DoubleSketch) getMaxItem() float64 {
	return k.maxDoubleItem
}

func (k *DoubleSketch) incN() {
	k.n++
}

func (k *DoubleSketch) setLevelZeroSorted(isSorted bool) {
	k.isLevelZeroSorted = isSorted
}

func (k *DoubleSketch) setLevelsArrayAt(index int, value int) {
	k.levelsArr[index] = value
}

func (k *DoubleSketch) setDoubleItemsArrayAt(index int, value float64) {
	k.doubleItems[index] = value
}

func (k *DoubleSketch) getNumLevels() int {
	if k.sketchStructure == sketchStructureEnum.updatable || k.sketchStructure == sketchStructureEnum.compactFull {
		return len(k.levelsArr) - 1
	}
	return 1
}

func (k *DoubleSketch) getDoubleItemsArray() []float64 {
	return k.doubleItems
}

func (k *DoubleSketch) setDoubleItemsArray(doubleItems []float64) {
	k.doubleItems = doubleItems
}

func (k *DoubleSketch) setNumLevels(numLevels int) {
	// no-op
}

func (k *DoubleSketch) setLevelsArray(levelsArr []int) error {
	if k.readOnly {
		return errors.New("Target sketch is Read Only, cannot write. ")
	}
	k.levelsArr = levelsArr
	return nil
}

func (k *DoubleSketch) getLevelsArray(structure sketchStructure) []int {
	if structure == sketchStructureEnum.updatable {
		res := make([]int, 0, len(k.levelsArr))
		copy(res, k.levelsArr)
		return res
	} else if structure == sketchStructureEnum.compactFull {
		res := make([]int, 0, len(k.levelsArr))
		for i := 0; i < len(k.levelsArr)-1; i++ {
			res[i] = k.levelsArr[i]
		}
		return res
	} else {
		return []int{}
	}
}

func updateDouble(dblSk *DoubleSketch, item float64) error {
	if math.IsNaN(item) {
		return nil
	}
	if dblSk.IsEmpty() {
		dblSk.setMinItem(item)
		dblSk.setMaxItem(item)
	} else {
		dblSk.setMinItem(math.Min(dblSk.getMinItem(), item))
		dblSk.setMaxItem(math.Max(dblSk.getMaxItem(), item))
	}
	level0space := dblSk.levelsArr[0]
	if level0space == 0 {
		err := dblSk.compressWhileUpdatingSketch()
		if err != nil {
			return err
		}
		level0space = dblSk.levelsArr[0]
	}
	dblSk.incN()
	dblSk.setLevelZeroSorted(false)
	nextPos := level0space - 1
	dblSk.setLevelsArrayAt(0, nextPos)
	dblSk.setDoubleItemsArrayAt(nextPos, item)
	return nil
}

func (k *DoubleSketch) compressWhileUpdatingSketch() error {
	level, err := findLevelToCompact(k.k, k.m, k.getNumLevels(), k.levelsArr)
	if err != nil {
		return err
	}
	if level == k.getNumLevels()-1 {
		//The level to compact is the top level, thus we need to add a level.
		//Be aware that this operation grows the items array,
		//shifts the items data and the level boundaries of the data,
		//and grows the levels array and increments numLevels_.
		err := k.addEmptyTopLevelToCompletelyFullSketch()
		if err != nil {
			return err
		}
	}
	//after this point, the levelsArray will not be expanded, only modified.
	myLevelsArr := k.levelsArr
	rawBeg := myLevelsArr[level]
	rawEnd := myLevelsArr[level+1]
	// +2 is OK because we already added a new top level if necessary
	popAbove := myLevelsArr[level+2] - rawEnd
	rawPop := rawEnd - rawBeg
	oddPop := (rawPop & 1) == 1 // isOdd
	adjBeg := rawBeg
	adjPop := rawPop
	if oddPop {
		adjBeg = rawBeg + 1
		adjPop = rawPop - 1
	}
	halfAdjPop := adjPop / 2

	//the following is specific to Doubles
	myDoubleItemsArr := k.doubleItems
	if level == 0 { // level zero might not be sorted, so we must sort it if we wish to compact it
		panic("implement me")
		// Arrays.sort(myDoubleItemsArr, adjBeg, adjBeg + adjPop);
	}
	if popAbove == 0 {
		randomlyHalveUpDoubles(myDoubleItemsArr, adjBeg, adjPop)
	} else {
		randomlyHalveDownDoubles(myDoubleItemsArr, adjBeg, adjPop)
		mergeSortedDoubleArrays(myDoubleItemsArr, adjBeg, halfAdjPop, myDoubleItemsArr, rawEnd, popAbove, myDoubleItemsArr, adjBeg+halfAdjPop)
	}

	newIndex := myLevelsArr[level+1] - halfAdjPop // adjust boundaries of the level above
	k.setLevelsArrayAt(level+1, newIndex)

	if oddPop {
		k.setLevelsArrayAt(level, myLevelsArr[level+1]-1)               // the current level now contains one item
		myDoubleItemsArr[myLevelsArr[level]] = myDoubleItemsArr[rawBeg] // namely this leftover guy
	} else {
		k.setLevelsArrayAt(level, myLevelsArr[level+1]) // the current level is now empty
	}

	// verify that we freed up halfAdjPop array slots just below the current level
	// assert myLevelsArr[level] == rawBeg + halfAdjPop;

	// finally, we need to shift up the data in the levels below
	// so that the freed-up space can be used by level zero
	if level > 0 {
		amount := rawBeg - myLevelsArr[0]
		for i := 0; i < amount; i++ {
			myDoubleItemsArr[myLevelsArr[0]+halfAdjPop+i] = myDoubleItemsArr[myLevelsArr[0]+i]
		}
	}
	for lvl := 0; lvl < level; lvl++ {
		newIndex = myLevelsArr[lvl] + halfAdjPop //adjust boundary
		k.setLevelsArrayAt(lvl, newIndex)
	}
	k.setDoubleItemsArray(myDoubleItemsArr)
	return nil
}

func (k *DoubleSketch) addEmptyTopLevelToCompletelyFullSketch() error {
	myCurLevelsArr := k.getLevelsArray(sketchStructureEnum.updatable)
	myCurNumLevels := k.getNumLevels()
	myCurTotalItemsCapacity := myCurLevelsArr[myCurNumLevels]

	var myNewNumLevels int
	var myNewLevelsArr []int
	var myNewTotalItemsCapacity int

	var myCurDoubleItemsArr []float64
	var myNewDoubleItemsArr []float64
	var minDouble = math.NaN()
	var maxDouble = math.NaN()

	myCurDoubleItemsArr = k.getDoubleItemsArray()
	minDouble = k.getMinItem()
	maxDouble = k.getMaxItem()
	//assert we are following a certain growth scheme
	if len(myCurDoubleItemsArr) != myCurTotalItemsCapacity {
		return errors.New("assert we are following a certain growth scheme")
	}

	if myCurLevelsArr[0] != 0 {
		return errors.New("definition of full is part of the growth scheme")
	}

	deltaItemsCap, err := levelCapacity(k.k, myCurNumLevels+1, 0, k.m)
	if err != nil {
		return err
	}
	myNewTotalItemsCapacity = myCurTotalItemsCapacity + deltaItemsCap

	// Check if growing the levels arr if required.
	// Note that merging MIGHT over-grow levels_, in which case we might not have to grow it
	growLevelsArr := myCurLevelsArr[myCurNumLevels+1] < myCurNumLevels+2

	// GROW LEVELS ARRAY
	if growLevelsArr {
		//grow levels arr by one and copy the old data to the new array, extra space at the top.
		myNewLevelsArr = make([]int, myCurNumLevels+2)
		copy(myNewLevelsArr, myCurLevelsArr)
		myNewNumLevels = myCurNumLevels + 1
	} else {
		myNewLevelsArr = myCurLevelsArr
		myNewNumLevels = myCurNumLevels
	}
	// This loop updates all level indices EXCLUDING the "extra" index at the top
	for level := 0; level <= myNewNumLevels-1; level++ {
		myNewLevelsArr[level] += deltaItemsCap
	}
	myNewLevelsArr[myNewNumLevels] = myNewTotalItemsCapacity // initialize the new "extra" index at the top
	myNewDoubleItemsArr = make([]float64, myNewTotalItemsCapacity)
	// copy and shift the current data into the new array
	for i := 0; i < myCurTotalItemsCapacity; i++ {
		myNewDoubleItemsArr[i+deltaItemsCap] = myCurDoubleItemsArr[i]
	}

	//update our sketch with new expanded spaces
	k.setNumLevels(myNewNumLevels)   //for off-heap only
	k.setLevelsArray(myNewLevelsArr) //the KllSketch copy
	k.setMinItem(minDouble)
	k.setMaxItem(maxDouble)
	k.setDoubleItemsArray(myNewDoubleItemsArr)

	return nil
}

func randomlyHalveUpDoubles(buf []float64, start int, length int) {
	halfLength := length / 2
	offset := rand.Intn(2) // disable for validation
	j := (start + length) - 1 - offset
	for i := (start + length) - 1; i >= (start + halfLength); i-- {
		buf[i] = buf[j]
		j -= 2
	}
}

func randomlyHalveDownDoubles(buf []float64, start int, length int) {
	halfLength := length / 2
	offset := rand.Intn(2) // disable for validation
	j := start + offset
	for i := start; i < (start + halfLength); i++ {
		buf[i] = buf[j]
		j += 2
	}
}

func mergeSortedDoubleArrays(
	bufA []float64, startA int, lenA int,
	bufB []float64, startB int, lenB int,
	bufC []float64, startC int,
) {
	lenC := lenA + lenB
	limA := startA + lenA
	limB := startB + lenB
	limC := startC + lenC

	a := startA
	b := startB

	for c := startC; c < limC; c++ {
		if a == limA {
			bufC[c] = bufB[b]
			b++
		} else if b == limB {
			bufC[c] = bufA[a]
			a++
		} else if bufA[a] < bufB[b] {
			bufC[c] = bufA[a]
			a++
		} else {
			bufC[c] = bufB[b]
			b++
		}
	}
}
