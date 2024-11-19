package connector

// Transformer ..
type Transformer[T any] interface {
	Transform(msg Message, to *T) error
}
