package router

import (
	"errors"
	"github.com/go-chi/jwtauth/v5"
	"net/http"

	"github.com/myrteametrics/myrtea-sdk/v4/handlers/render"
	"go.uber.org/zap"
)

// UnverifiedAuthenticator doc
// WARNING: Don't use this method unless you know what you're doing
// This method parses the token but doesn't validate the signature. It's only
// ever useful in cases where you know the signature is valid (because it has
// been checked previously in the stack) and you want to extract values from
// it.
func UnverifiedAuthenticator(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		findTokenFns := []func(r *http.Request) string{jwtauth.TokenFromQuery, jwtauth.TokenFromHeader, jwtauth.TokenFromCookie}
		var tokenStr string
		for _, fn := range findTokenFns {
			if tokenStr = fn(r); tokenStr != "" {
				break
			}
		}
		if tokenStr == "" {
			zap.L().Warn("No JWT string found in request")
			render.Error(w, r, render.ErrAPISecurityMissingContext, errors.New("missing JWT"))
			return
		}

		auth := &jwtauth.JWTAuth{}
		token, err := auth.Decode(tokenStr)
		if err != nil {
			zap.L().Warn("JWT string cannot be parsed") // , zap.String("jwt", tokenStr)) // Security issue if logged without check ?
			render.Error(w, r, render.ErrAPISecurityMissingContext, errors.New("invalid JWT"))
			return
		}

		ctx = jwtauth.NewContext(ctx, token, err)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
