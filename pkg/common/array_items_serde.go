package common

type ArrayOfItemsSerDe[C any] interface {
	DeserializeFromSlice(bytes []byte, numItems int32) []C
	ToSliceFromOne(item C) []byte
	ToSliceFromMany(item []C) []byte
	SizeOfOne(item C) int32
	SizeOfMany(item []C) int32
	String(item C) string
}
