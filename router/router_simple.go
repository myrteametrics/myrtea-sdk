package router

import (
	"github.com/golang-jwt/jwt/v4"
	"github.com/myrteametrics/myrtea-sdk/v4/connector"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/go-chi/jwtauth"
	"github.com/myrteametrics/myrtea-sdk/v4/handlers"
	"github.com/myrteametrics/myrtea-sdk/v4/postgres"
	"github.com/myrteametrics/myrtea-sdk/v4/security"

	stdprometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	httpSwagger "github.com/swaggo/http-swagger"
	"go.uber.org/zap"
)

// ConfigSimple wraps common configuration parameters
type ConfigSimple struct {
	Production              bool
	Security                bool
	CORS                    bool
	GatewayMode             bool
	VerboseError            bool
	Reloader                *connector.Reloader
	Restarter               *connector.Restarter
	AuthenticationMode      string
	LogLevel                zap.AtomicLevel
	MetricsNamespace        string
	MetricsPrometheusLabels stdprometheus.Labels
	MetricsServiceName      string
	PublicRoutes            map[string]http.Handler
	ProtectedRoutes         map[string]http.Handler
}

// Check clean up the configuration and logs comments if required
func (config *ConfigSimple) Check() {
	if !config.Security {
		zap.L().Warn("API starting in unsecured mode, be sure to set API_ENABLE_SECURITY=true in production")
	}
	if config.VerboseError {
		zap.L().Warn("API starting in verbose error mode, be sure to set API_ENABLE_VERBOSE_MODE=false in production")
	}
	if config.GatewayMode {
		zap.L().Warn("Server router will be started using API Gateway mode. " +
			"Please ensure every request has been properly pre-verified by the auth-api")
		if !config.Security {
			zap.L().Warn("Gateway mode has no use if API security is not enabled (API_ENABLE_SECURITY=false)")
			config.GatewayMode = false
		}
	}
	if config.Security && config.GatewayMode && config.AuthenticationMode == "SAML" {
		zap.L().Warn("SAML Authentication mode is not compatible with API_ENABLE_GATEWAY_MODE=true")
		config.GatewayMode = false
	}
	if config.AuthenticationMode != "BASIC" && config.AuthenticationMode != "SAML" {
		zap.L().Warn("Authentication mode not supported. Back to default value 'BASIC'", zap.String("AuthenticationMode", config.AuthenticationMode))
		config.AuthenticationMode = "BASIC"
	}
}

// NewChiRouterSimple initialize a chi.Mux router with all required default middleware (logger, security, recovery, etc.)
func NewChiRouterSimple(config ConfigSimple) *chi.Mux {
	config.Check()

	r := chi.NewRouter()
	// Global middleware stack
	// TODO: Add CORS middleware
	if config.CORS {
		cors := cors.New(cors.Options{
			AllowedOrigins:   []string{"*"},
			AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
			AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
			ExposedHeaders:   []string{"Link", "Authenticate-To"},
			AllowCredentials: true,
			MaxAge:           300, // Maximum value not ignored by any of major browsers
		})
		r.Use(cors.Handler)
	}

	r.Use(middleware.SetHeader("Strict-Transport-Security", "max-age=63072000; includeSubDomains"))
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.StripSlashes)
	r.Use(middleware.RedirectSlashes)
	if config.Production {
		r.Use(CustomZapLogger)
	} else {
		r.Use(CustomLogger)
	}
	r.Use(NewMetricMiddleware(config.MetricsNamespace, config.MetricsPrometheusLabels, config.MetricsServiceName))
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))

	// Specific security middleware initialization
	signingKey := []byte(security.RandString(128))
	securityMiddleware := security.NewMiddlewareJWT(signingKey, security.NewDatabaseAuth(postgres.DB()))

	// if prometheus
	r.Handle("/metrics", promhttp.HandlerFor(stdprometheus.DefaultGatherer, promhttp.HandlerOpts{}))

	r.Route("/api/v1", func(r chi.Router) {

		// Public routes
		r.Group(func(rg chi.Router) {
			rg.Get("/isalive", handlers.IsAlive)
			rg.Post("/login", securityMiddleware.GetToken())
			rg.Get("/swagger/*", httpSwagger.WrapHandler)

			if config.Reloader != nil {
				config.Reloader.BindEndpoint(rg)
			}

			if config.Restarter != nil {
				config.Restarter.BindEndpoint(rg)
			}

			for path, handler := range config.PublicRoutes {
				rg.Mount(path, handler)
			}
		})

		// Protected routes
		r.Group(func(rg chi.Router) {
			if config.Security {
				if config.GatewayMode {
					// Warning: No signature verification will be done on JWT.
					// JWT MUST have been verified before by the API Gateway
					rg.Use(UnverifiedAuthenticator)
				} else {
					rg.Use(jwtauth.Verifier(jwtauth.New(jwt.SigningMethodHS256.Name, signingKey, nil)))
					rg.Use(jwtauth.Authenticator)
				}
				// rg.Use(ContextMiddleware)
			}
			rg.Use(middleware.SetHeader("Content-Type", "application/json"))

			rg.HandleFunc("/log_level", config.LogLevel.ServeHTTP)

			for path, handler := range config.ProtectedRoutes {
				rg.Mount(path, handler)
			}
		})
	})

	return r
}
