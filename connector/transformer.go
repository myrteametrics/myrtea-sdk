package connector

// Transformer ..
type Transformer interface {
	Transform(msg Message, to *any) error
}
