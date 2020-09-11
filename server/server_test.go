package server

import (
	"net/http"
)

// TestEndpoint returns a basic message when called
var TestEndpoint = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte("Test Endpoint"))
})

// TODO: Server package refactoring using gorouting + channel for easier testing and usage
// func TestServerRun(t *testing.T) {
// 	s := NewUnsecuredServer(9000, nil)
// 	defer s.Close()
// 	err := s.Run()
// 	if err != nil {
// 		t.Error(err)
// 	}
// }
