package ssdb

type ValueType int8

const (
	TypeDeletion = ValueType(iota)
	TypeValue
)

type Closer interface {
	Close()
}

type Clearer interface {
	Clear()
}
