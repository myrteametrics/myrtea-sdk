package security

// Auth refers to a generic interface which must be implemented by every authentication backend
type Auth interface {
	Authenticate(string, string) (bool, User, error)
}
