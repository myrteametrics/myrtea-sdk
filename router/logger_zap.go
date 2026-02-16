package router

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	chimiddleware "github.com/go-chi/chi/v5/middleware"
	gorillacontext "github.com/gorilla/context"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ZapFieldEnricher is a function type that can enrich log fields
// It receives the request and current fields, and returns additional fields to append
type ZapFieldEnricher func(r *http.Request) []zapcore.Field

var (
	// ZapRequestLogger is called by the Logger middleware handler to log each request.
	// Its made a package-level variable so that it can be reconfigured for custom
	// logging configurations.
	ZapRequestLogger = CustomZapRequestLogger(&CustomZapLogFormatter{Logger: log.New(os.Stdout, "", log.LstdFlags), NoColor: false})

	// zapFieldEnrichers holds the list of field enrichment functions
	// These functions are called when logging to add custom fields
	zapFieldEnrichers = make([]ZapFieldEnricher, 0)
)

// RegisterZapFieldEnricher registers a function that will be called to enrich log fields.
// Enrichers are called in the order they were registered.
// This allows modules importing the SDK to add custom fields based on the request or context.
//
// Example usage:
//
//	router.RegisterZapFieldEnricher(func(r *http.Request) []zapcore.Field {
//	    if tenantID := r.Context().Value("tenant_id"); tenantID != nil {
//	        return []zapcore.Field{zap.String("tenant_id", tenantID.(string))}
//	    }
//	    return nil
//	})
func RegisterZapFieldEnricher(enricher ZapFieldEnricher) {
	zapFieldEnrichers = append(zapFieldEnrichers, enricher)
}

// ClearZapFieldEnrichers removes all registered field enrichers.
// Useful for testing or reconfiguration.
func ClearZapFieldEnrichers() {
	zapFieldEnrichers = make([]ZapFieldEnricher, 0)
}

// GetZapFieldEnrichers returns a copy of the registered enrichers.
// Useful for testing or debugging.
func GetZapFieldEnrichers() []ZapFieldEnricher {
	enrichers := make([]ZapFieldEnricher, len(zapFieldEnrichers))
	copy(enrichers, zapFieldEnrichers)
	return enrichers
}

// CustomZapLogger is a middleware that logs the start and end of each request, along
// with some useful data about what was requested, what the response status was,
// and how long it took to return. When standard output is a TTY, Logger will
// print in color, otherwise it will print in black and white. Logger prints a
// request ID if one is provided.
//
// Alternatively, look at https://github.com/pressly/lg and the `lg.RequestLogger`
// middleware pkg.
func CustomZapLogger(next http.Handler) http.Handler {
	return ZapRequestLogger(next)
}

// CustomZapRequestLogger returns a logger handler using a custom LogFormatter.
func CustomZapRequestLogger(f chimiddleware.LogFormatter) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			entry := f.NewLogEntry(r)
			ww := chimiddleware.NewWrapResponseWriter(w, r.ProtoMajor)

			t1 := time.Now()
			defer func() {
				user := gorillacontext.Get(r, UserLogin)
				gorillacontext.Clear(r)
				zapFields := entry.(*customZapLogEntry).ZapFields
				if user != nil {
					zapFields = append(zapFields, zap.String("myrtea_user", user.(string)))
				}

				// Apply custom field enrichers
				for _, enricher := range zapFieldEnrichers {
					if additionalFields := enricher(r); additionalFields != nil {
						zapFields = append(zapFields, additionalFields...)
					}
				}

				zapFields = append(zapFields,
					zap.Duration("lat", time.Since(t1)),
					zap.Int("status", ww.Status()),
					zap.Int("size", ww.BytesWritten()),
				)

				entry.(*customZapLogEntry).ZapFields = zapFields
				entry.Write(ww.Status(), ww.BytesWritten(), ww.Header(), time.Since(t1), nil)

			}()
			ctx := context.WithValue(r.Context(), ContextKeyLoggerR, r)
			next.ServeHTTP(ww, chimiddleware.WithLogEntry(r.WithContext(ctx), entry))
		}
		return http.HandlerFunc(fn)
	}
}

// CustomZapLogFormatter is a simple logger that implements a LogFormatter.
type CustomZapLogFormatter struct {
	Logger  chimiddleware.LoggerInterface
	NoColor bool
}

// NewLogEntry creates a new LogEntry for the request.
func (l *CustomZapLogFormatter) NewLogEntry(r *http.Request) chimiddleware.LogEntry {
	entry := &customZapLogEntry{
		CustomZapLogFormatter: l,
		request:               r,
		ZapLogger:             zap.L(),
		ZapFields:             make([]zapcore.Field, 0),
	}

	reqID := chimiddleware.GetReqID(r.Context())
	if reqID != "" {
		entry.ZapFields = append(entry.ZapFields, zap.String("requestid", reqID))
	}

	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}

	entry.ZapFields = append(entry.ZapFields,
		zap.String("method", r.Method),
		zap.String("scheme", scheme),
		zap.String("myrtea_host", r.Host),
		zap.String("path", r.RequestURI),
		zap.String("proto", r.Proto),
		zap.String("remoteaddr", r.RemoteAddr),
	)

	return entry
}

type customZapLogEntry struct {
	*CustomZapLogFormatter
	request   *http.Request
	ZapLogger *zap.Logger
	ZapFields []zap.Field
}

func (l *customZapLogEntry) Write(status, bytes int, header http.Header, elapsed time.Duration, extra interface{}) {
	l.ZapLogger.Info("request served", l.ZapFields...)
}

func (l *customZapLogEntry) Panic(v interface{}, stack []byte) {
	l.ZapLogger.Panic("request served", zap.Any("reason", v), zap.String("stack", string(stack)))
}
