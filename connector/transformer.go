package connector

// Transformer ..
type Transformer interface {
	Transform(msg Message) (Message, error)
}
