package kll

import (
	"errors"
	"fmt"
	"math"
)

const (
	_DEFAULT_K = 200
	_DEFAULT_M = 8
	_MAX_K     = (1 << 16) - 1
	_MAX_M     = 8 //The maximum M
	_MIN_M     = 2 // The minimum M
)

const (
	_PMF_COEF = 2.446
	_PMF_EXP  = 0.9433
	_CDF_COEF = 2.296
	_CDF_EXP  = 0.9723
)

var (
	powersOfThree = []int64{1, 3, 9, 27, 81, 243, 729, 2187, 6561, 19683, 59049, 177147, 531441,
		1594323, 4782969, 14348907, 43046721, 129140163, 387420489, 1162261467,
		3486784401, 10460353203, 31381059609, 94143178827, 282429536481,
		847288609443, 2541865828329, 7625597484987, 22876792454961, 68630377364883,
		205891132094649}
)

func checkK(k, m int) error {
	if k < m || k > _MAX_K {
		return fmt.Errorf("K must be >= %d and <= %d: %d", m, _MAX_K, k)
	}
	return nil
}

func checkM(m int) error {
	if m < _MIN_M || m > _MAX_M || ((m & 1) == 1) {
		return fmt.Errorf("M must be >= 2, <= 8 and even: %d", m)
	}
	return nil
}

func checkNormalizedRankBounds(nRank float64) error {
	if nRank < 0.0 || nRank > 1.0 {
		return fmt.Errorf("A normalized rank must be >= 0 and <= 1.0: %f", nRank)
	}
	return nil
}

func checkDoublesSplitPointsOrder(values []float64) error {
	if len(values) == 1 && values[0] != values[0] {
		return errors.New("Values must be unique, monotonically increasing and not NaN.")
	}
	for j := 0; j < len(values)-1; j++ {
		if values[j] < values[j+1] {
			continue
		}
		return errors.New("Values must be unique, monotonically increasing and not NaN.")
	}
	return nil
}

func getNormalizedRankError(minK int, pmf bool) float64 {
	if pmf {
		return _PMF_COEF / math.Pow(float64(minK), _PMF_EXP)
	}
	return _CDF_COEF / math.Pow(float64(minK), _CDF_EXP)
}

func toStringImpl(sketch *DoubleSketch, withSummary, withData bool) string {
	k := sketch.GetK()
	m := sketch.GetM()
	n := sketch.GetN()
	numLevels := sketch.getNumLevels()
	fullLevelsArr := sketch.getLevelsArray(sketchStructureEnum.updatable)
	epsPct := fmt.Sprintf("%.3f%%", sketch.GetNormalizedRankError(false)*100)
	epsPMFPct := fmt.Sprintf("%.3f%%", sketch.GetNormalizedRankError(true)*100)

	sb := ""
	sb += fmt.Sprintf("### KllDoubleSketch Summary:\n")
	sb += fmt.Sprintf("   K                      : %d\n", k)
	sb += fmt.Sprintf("   Dynamic min K          : %d\n", sketch.GetMinK())
	sb += fmt.Sprintf("   M                      : %d\n", m)
	sb += fmt.Sprintf("   N                      : %d\n", n)
	sb += fmt.Sprintf("   Epsilon                : %s\n", epsPct)
	sb += fmt.Sprintf("   Epsilon PMF            : %s\n", epsPMFPct)
	sb += fmt.Sprintf("   Empty                  : %t\n", sketch.IsEmpty())
	sb += fmt.Sprintf("   Estimation Mode        : %t\n", sketch.isEstimationMode())
	sb += fmt.Sprintf("   Levels                 : %d\n", numLevels)
	sb += fmt.Sprintf("   Level 0 Sorted         : %t\n", sketch.IsLevelZeroSorted())
	sb += fmt.Sprintf("   Capacity Items         : %d\n", fullLevelsArr[numLevels])
	sb += fmt.Sprintf("   Retained Items         : %d\n", sketch.GetNumRetained())
	sb += fmt.Sprintf("   Empty/Garbage Items    : %d\n", sketch.levelsArr[0])
	sb += fmt.Sprintf("   ReadOnly               : false\n")
	//sb += fmt.Sprintf("   Updatable Storage Bytes: %d\n", sketch.CurrentSerializedSizeBytes(true))
	//sb += fmt.Sprintf("   Compact Storage Bytes  : %d\n", sketch.CurrentSerializedSizeBytes(false))

	if sketch.IsEmpty() {
		emptyStr := "NaN"
		sb += fmt.Sprintf("   Min Item               : %s\n", emptyStr)
		sb += fmt.Sprintf("   Max Item               : %s\n", emptyStr)
	} else {
		minItem, _ := sketch.GetMinItem()
		sb += fmt.Sprintf("   Min Item               : %f\n", minItem)
		maxItem, _ := sketch.GetMaxItem()
		sb += fmt.Sprintf("   Max Item               : %f\n", maxItem)
	}

	sb += fmt.Sprintf("### End sketch summary\n")

	if !withSummary {
		sb = ""
	}
	//if withData {
	//	sb += outputData(sketch)
	//}
	return sb
}

func findLevelToCompact(k int, m int, numLevels int, levels []int) (int, error) {
	level := 0
	for {
		if level >= numLevels {
			return 0, errors.New("level >= numLevels")
		}
		pop := levels[level+1] - levels[level]
		capacity, err := levelCapacity(k, numLevels, level, m)
		if err != nil {
			return 0, err
		}
		if pop >= capacity {
			return level, nil
		}
		level++
	}
}

func levelCapacity(k int, numLevels int, level int, m int) (int, error) {
	if k > (1 << 29) {
		return 0, errors.New("k > (1 << 29)")
	}
	if numLevels < 1 || numLevels > 61 {
		return 0, errors.New("numLevels < 1 || numLevels > 61")
	}
	if level < 0 || level >= numLevels {
		return 0, errors.New("level < 0 || level >= numLevels")
	}
	depth := numLevels - level - 1
	v, err := intCapAux(k, depth)
	if err != nil {
		return 0, err
	}
	return max(m, int(v)), nil
}

func intCapAux(k int, depth int) (int64, error) {
	if depth <= 30 {
		return intCapAuxAux(int64(k), depth)
	}
	half := depth / 2
	rest := depth - half
	tmp, err := intCapAuxAux(int64(k), half)
	if err != nil {
		return 0, err
	}
	return intCapAuxAux(tmp, rest)
}

func intCapAuxAux(k int64, depth int) (int64, error) {
	twok := k << 1
	tmp := (twok << depth) / powersOfThree[depth]
	result := (tmp + 1) >> 1
	if result > k {
		return 0, errors.New("result > k")
	}
	return result, nil
}

func convertToCumulative(array []int64) ([]int64, int64) {
	subtotal := int64(0)
	for i := 0; i < len(array); i++ {
		newSubtotal := subtotal + array[i]
		subtotal = array[i]
		array[i] = newSubtotal
	}
	return array, subtotal
}
