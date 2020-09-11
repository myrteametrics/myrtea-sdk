package engine

// Token interface for the fact fragment tokens
type Token interface {
	String() string
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error
}
