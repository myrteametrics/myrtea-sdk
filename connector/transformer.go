package connector

// Transformer ..
type Transformer interface {
	Transform(Message) (Message, error)
}
