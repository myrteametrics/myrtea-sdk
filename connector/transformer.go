package connector

// Transformer ..
type Transformer[T any] interface {
	Transform(msg Message, T *any) error
}
