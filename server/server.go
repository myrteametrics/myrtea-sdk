package server

import (
	"crypto/tls"
	"net/http"
	"strconv"

	"go.uber.org/zap"
)

// Server is a wrapper of standard http.Server
type Server struct {
	server  *http.Server
	secured bool
	cert    string
	key     string
}

// NewSecuredServer returns a pointer to a new instance of Server
func NewSecuredServer(port int, cert string, key string, router http.Handler) *http.Server {
	tlsConfig := &tls.Config{
		MinVersion:               tls.VersionTLS12,
		CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		},
	}
	server := &http.Server{
		Addr:         ":" + strconv.Itoa(port),
		Handler:      router,
		TLSConfig:    tlsConfig,
		TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler), 0),
	}

	return server
}

// NewUnsecuredServer returns a pointer to a new instance of Server, without any SSL security
func NewUnsecuredServer(port int, router http.Handler) *http.Server {
	server := &http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: router,
	}

	return server
}

// Run starts the server
func (s *Server) Run() error {
	zap.L().Info("Starting server", zap.String("Addr", s.server.Addr))
	var err error
	if s.secured {
		err = s.server.ListenAndServeTLS(s.cert, s.key)
	} else {
		err = s.server.ListenAndServe()
	}
	if err != nil {
		zap.L().Error("Error when starting server", zap.Error(err))
		return err
	}
	return nil
}

// Close closes the server
func (s *Server) Close() error {
	err := s.server.Close()
	if err != nil {
		return err
	}
	return nil
}

/*
// CORSRouterDecorator applies CORS headers to a mux.Router
type CORSRouterDecorator struct {
	R *mux.Router
}

// ServeHTTP wraps the HTTP server enabling CORS headers.
// For more info about CORS, visit https://www.w3.org/TR/cors/
func (c *CORSRouterDecorator) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if origin := req.Header.Get("Origin"); origin != "" {
		rw.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		rw.Header().Set("Access-Control-Allow-Origin", origin)
		rw.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		rw.Header().Set("Access-Control-Allow-Headers", "Accept, Accept-Language, Content-Type, text/plain")
		rw.Header().Set("Content-Type", "application/json")
	}
	if req.Method == "OPTIONS" {
		return
	}
	c.R.ServeHTTP(rw, req)
}
*/
