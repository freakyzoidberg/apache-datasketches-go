package kll

import (
	"errors"
	"sort"
)

type doubleSketchSortedView struct {
	quantiles  []float64
	cumWeights []int64 //comes in as individual weights, converted to cumulative natural weights
	totalN     int64
	maxItem    float64
	minItem    float64
}

func newDoubleSketchSortedViewFromSketch(sketch *DoubleSketch) (*doubleSketchSortedView, error) {
	if sketch.IsEmpty() {
		panic("sketch.IsEmpty()")
	}
	totalN := sketch.GetN()
	maxItem, err := sketch.GetMaxItem()
	if err != nil {
		return nil, err
	}
	minItem, err := sketch.GetMinItem()
	if err != nil {
		return nil, err
	}
	srcQuantiles := sketch.getDoubleItemsArray()
	srcLevels := sketch.levelsArr
	srcNumLevels := sketch.getNumLevels()

	if !sketch.IsLevelZeroSorted() {
		sort.Float64s(srcQuantiles[srcLevels[0]:srcLevels[1]])
	}

	numQuantiles := srcLevels[srcNumLevels] - srcLevels[0] //remove garbage
	quantiles, cumWeights := populateFromSketch(srcQuantiles, srcLevels, srcNumLevels, numQuantiles)
	return &doubleSketchSortedView{
		quantiles,
		cumWeights,
		totalN,
		maxItem,
		minItem}, nil
}

func (v *doubleSketchSortedView) IsEmpty() bool {
	return v.totalN == 0
}

func (v *doubleSketchSortedView) getQuantile(rank float64, searchCriteria KllSearchCriteria) (float64, error) {
	if v.IsEmpty() {
		return 0, errors.New("v.IsEmpty()")
	}
	if err := checkNormalizedRankBounds(rank); err != nil {
		return 0, err
	}
	length := len(v.cumWeights)
	panic("not implemented")
	//naturalRank := getNaturalRank(rank, v.totalN, searchCriteria)
	//crit := InequalitySearchGE
	//if searchCriteria == INCLUSIVE {
	//	crit = InequalitySearchLE
	//}
	//index := InequalitySearch(v.cumWeights, 0, len-1, naturalRank, crit)
	index := 0
	if index == -1 {
		return v.quantiles[length-1], nil //EXCLUSIVE (GT) case: normRank == 1.0;
	}
	return v.quantiles[index], nil
}

func (v *doubleSketchSortedView) getPMF(splitPoints []float64, searchCriteria KllSearchCriteria) ([]float64, error) {
	buckets, err := v.getCDF(splitPoints, searchCriteria)
	if err != nil {
		return nil, err
	}
	for i := len(buckets); i > 1; i-- {
		buckets[i] -= buckets[i-1]
	}
	return buckets, nil
}

func (v *doubleSketchSortedView) getCDF(splitPoints []float64, searchCriteria KllSearchCriteria) ([]float64, error) {
	if err := checkDoublesSplitPointsOrder(splitPoints); err != nil {
		return nil, err
	}
	var (
		leng = len(splitPoints) + 1
		err  error
	)
	buckets := make([]float64, leng)
	for i := 0; i < leng-1; i++ {
		buckets[i], err = v.getRank(splitPoints[i], searchCriteria)
		if err != nil {
			return nil, err
		}
	}
	buckets[leng-1] = 1.0
	return buckets, nil
}

func (v *doubleSketchSortedView) getRank(quantile float64, searchCriteria KllSearchCriteria) (float64, error) {
	if v.IsEmpty() {
		return 0, errors.New("v.IsEmpty()")
	}
	panic("not implemented")
	//leng := len(v.quantiles)
	//crit := InequalitySearchLE
	//if searchCriteria == EXCLUSIVE {
	//	crit = InequalitySearchLT
	//}
	//index := InequalitySearch(v.quantiles, 0, leng-1, quantile, crit)
	index := 0
	if index == -1 {
		return 0, nil //EXCLUSIVE (LT) case: quantile <= minQuantile; INCLUSIVE (LE) case: quantile < minQuantile
	}
	return float64(v.cumWeights[index]) / float64(v.totalN), nil
}

func populateFromSketch(srcQuantiles []float64, srcLevels []int, srcNumLevels int, numQuantiles int) ([]float64, []int64) {
	quantiles := make([]float64, numQuantiles)
	cumWeights := make([]int64, numQuantiles)

	myLevels := make([]int, srcNumLevels+1)
	offset := srcLevels[0]
	copy(myLevels, srcLevels)
	copy(srcQuantiles, srcQuantiles[offset:offset+numQuantiles])
	srcLevel := 0
	dstLevel := 0
	weight := int64(1)
	for srcLevel < srcNumLevels {
		fromIndex := srcLevels[srcLevel] - offset
		toIndex := srcLevels[srcLevel+1] - offset // exclusive
		if fromIndex < toIndex {                  // if equal, skip empty level
			for i := fromIndex; i < toIndex; i++ {
				cumWeights[i] = weight
			}
			myLevels[dstLevel] = fromIndex
			myLevels[dstLevel+1] = toIndex
			dstLevel++
		}
		srcLevel++
		weight *= 2
	}
	numLevels := dstLevel
	quantiles, cumWeights = blockyTandemMergeSort(quantiles, cumWeights, myLevels, numLevels) //create unit weights
	cumWeights, _ = convertToCumulative(cumWeights)
	return quantiles, cumWeights

}

func blockyTandemMergeSort(quantiles []float64, weights []int64, levels []int, numLevels int) ([]float64, []int64) {
	if numLevels == 1 {
		return quantiles, weights
	}
	// duplicate the input in preparation for the "ping-pong" copy reduction strategy.
	quantilesTmp := make([]float64, len(quantiles))
	copy(quantilesTmp, quantiles)
	weightsTmp := make([]int64, len(weights))
	copy(weightsTmp, weights)

	return blockyTandemMergeSortRecursion(quantilesTmp, weightsTmp, quantiles, weights, levels, 0, numLevels)
}

func blockyTandemMergeSortRecursion(quantilesSrc []float64, weightsSrc []int64, quantilesDst []float64, weightsDst []int64, levels []int, startingLevel int, numLevels int) ([]float64, []int64) {
	if numLevels == 1 {
		return quantilesDst, weightsDst
	}
	numLevels1 := numLevels / 2
	numLevels2 := numLevels - numLevels1
	if numLevels1 < 1 {
		panic("numLevels1 < 1")
	}
	if numLevels2 < numLevels1 {
		panic("numLevels2 < numLevels1")
	}
	startingLevel1 := startingLevel
	startingLevel2 := startingLevel + numLevels1
	// swap roles of src and dst
	quantilesDst, weightsDst = blockyTandemMergeSortRecursion(quantilesDst, weightsDst, quantilesSrc, weightsSrc, levels, startingLevel1, numLevels1)
	quantilesDst, weightsDst = blockyTandemMergeSortRecursion(quantilesDst, weightsDst, quantilesSrc, weightsSrc, levels, startingLevel2, numLevels2)
	return tandemMerge(quantilesSrc, weightsSrc, quantilesDst, weightsDst, levels, startingLevel1, numLevels1, startingLevel2, numLevels2)
}

func tandemMerge(quantilesSrc []float64, weightsSrc []int64, quantilesDst []float64, weightsDst []int64, levelStarts []int, startingLevel1 int, numLevels1 int, startingLevel2 int, numLevels2 int) ([]float64, []int64) {
	fromIndex1 := levelStarts[startingLevel1]
	toIndex1 := levelStarts[startingLevel1+numLevels1] // exclusive
	fromIndex2 := levelStarts[startingLevel2]
	toIndex2 := levelStarts[startingLevel2+numLevels2] // exclusive
	iSrc1 := fromIndex1
	iSrc2 := fromIndex2
	iDst := fromIndex1

	for iSrc1 < toIndex1 && iSrc2 < toIndex2 {
		if quantilesSrc[iSrc1] < quantilesSrc[iSrc2] {
			quantilesDst[iDst] = quantilesSrc[iSrc1]
			weightsDst[iDst] = weightsSrc[iSrc1]
			iSrc1++
		} else {
			quantilesDst[iDst] = quantilesSrc[iSrc2]
			weightsDst[iDst] = weightsSrc[iSrc2]
			iSrc2++
		}
		iDst++
	}
	if iSrc1 < toIndex1 {
		copy(quantilesDst[iDst:], quantilesSrc[iSrc1:toIndex1])
		copy(weightsDst[iDst:], weightsSrc[iSrc1:toIndex1])
	} else if iSrc2 < toIndex2 {
		copy(quantilesDst[iDst:], quantilesSrc[iSrc2:toIndex2])
		copy(weightsDst[iDst:], weightsSrc[iSrc2:toIndex2])
	}

	return quantilesDst, weightsDst
}
