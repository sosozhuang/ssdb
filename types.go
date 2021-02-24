package ssdb

type ValueType int8

const (
	TypeDeletion = ValueType(iota)
	TypeValue
)
