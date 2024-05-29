package security

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"time"

	"github.com/go-chi/jwtauth/v5"

	"go.uber.org/zap"
)

// Middleware is an interface for standard http middleware
type Middleware interface {
	Handler(h http.Handler) http.Handler
}

// MiddlewareJWT is an implementation of Middleware interface, which provides a specific security handler based on JWT (JSON Web Token)
type MiddlewareJWT struct {
	Auth       Auth
	Handler    func(h http.Handler) http.Handler
	signingKey []byte
	JwtAuth    *jwtauth.JWTAuth
}

// JwtToken wrap the json web token string
type JwtToken struct {
	Token string `json:"token"`
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

// RandStringWithCharset generate a random string with a specific charset
func RandStringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

// RandString generate a random string with the default charset ([A-Za-z])
func RandString(length int) string {
	return RandStringWithCharset(length, charset)
}

// NewMiddlewareJWT initialize a new instance of MiddlewareJWT and returns a pointer of it
func NewMiddlewareJWT(jwtSigningKey []byte, auth Auth) *MiddlewareJWT {
	tokenAuth := jwtauth.New("HS256", jwtSigningKey, nil)
	return &MiddlewareJWT{auth, nil, jwtSigningKey, tokenAuth}
}

// GetToken returns a http.Handler to authenticate and get a JWT
func (middleware *MiddlewareJWT) GetToken() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var credentials UserWithPassword
		err := json.NewDecoder(r.Body).Decode(&credentials)
		if err != nil {
			zap.L().Error("GetToken.Decode:", zap.Error(err))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		allowed, user, err := middleware.Auth.Authenticate(credentials.Login, credentials.Password)
		if err != nil {
			zap.L().Error("Authentication failed", zap.Error(err))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if !allowed {
			zap.L().Error("Invalid credentials")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		_, tokenString, err := middleware.JwtAuth.Encode(map[string]interface{}{
			"iss":  "Myrtea metrics",
			"exp":  time.Now().Add(time.Hour * 12).Unix(),
			"iat":  time.Now().Unix(),
			"nbf":  time.Now().Unix(),
			"role": user.Role,
			"id":   user.ID,
		})
		if err != nil {
			zap.L().Error("Error while signing token", zap.Error(err))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err = json.NewEncoder(w).Encode(JwtToken{Token: tokenString})
		if err != nil {
			zap.L().Error("Error while encoding the token ", zap.Error(err))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}
}

// AdminAuthentificator is a middle which check if the user is administrator (role=1)
func AdminAuthentificator(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// get user infos from token
		_, claims, _ := jwtauth.FromContext(r.Context())
		// test if user haven't right to access
		if claims["role"] != float64(1) {
			http.Error(w, http.StatusText(403), 403)
			return
		}
		next.ServeHTTP(w, r)
	})
}
