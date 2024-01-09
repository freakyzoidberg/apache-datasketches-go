package kll

import (
	"errors"
	"fmt"
)

const (
	_DEFAULT_K = 200
	_DEFAULT_M = 8
	_MAX_K     = (1 << 16) - 1
	_MAX_M     = 8 //The maximum M
	_MIN_M     = 2 // The minimum M
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
