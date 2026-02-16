package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestRegisterZapFieldEnricher(t *testing.T) {
	// Clear any existing enrichers before test
	ClearZapFieldEnrichers()

	// Test registering an enricher
	enricher := func(r *http.Request) []zapcore.Field {
		if tenantID := r.Context().Value("tenant_id"); tenantID != nil {
			return []zapcore.Field{zap.String("tenant_id", tenantID.(string))}
		}
		return nil
	}

	RegisterZapFieldEnricher(enricher)

	enrichers := GetZapFieldEnrichers()
	if len(enrichers) != 1 {
		t.Errorf("Expected 1 enricher, got %d", len(enrichers))
	}
}

func TestClearZapFieldEnrichers(t *testing.T) {
	// Register some enrichers
	RegisterZapFieldEnricher(func(r *http.Request) []zapcore.Field {
		return []zapcore.Field{zap.String("test", "value")}
	})

	// Clear them
	ClearZapFieldEnrichers()

	enrichers := GetZapFieldEnrichers()
	if len(enrichers) != 0 {
		t.Errorf("Expected 0 enrichers after clear, got %d", len(enrichers))
	}
}

func TestZapFieldEnricherWithContext(t *testing.T) {
	// Clear any existing enrichers
	ClearZapFieldEnrichers()

	// Register an enricher that extracts from context
	RegisterZapFieldEnricher(func(r *http.Request) []zapcore.Field {
		fields := make([]zapcore.Field, 0)

		if tenantID := r.Context().Value("tenant_id"); tenantID != nil {
			fields = append(fields, zap.String("tenant_id", tenantID.(string)))
		}

		if userID := r.Context().Value("user_id"); userID != nil {
			fields = append(fields, zap.String("user_id", userID.(string)))
		}

		return fields
	})

	// Create a test request with context values
	req := httptest.NewRequest("GET", "http://example.com/test", nil)
	ctx := context.WithValue(req.Context(), "tenant_id", "tenant-123")
	ctx = context.WithValue(ctx, "user_id", "user-456")
	req = req.WithContext(ctx)

	// Test that the enricher is called (we can't test the actual logging here,
	// but we can verify the enricher returns the expected fields)
	enrichers := GetZapFieldEnrichers()
	if len(enrichers) != 1 {
		t.Fatalf("Expected 1 enricher, got %d", len(enrichers))
	}

	fields := enrichers[0](req)
	if len(fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(fields))
	}
}

func TestMultipleZapFieldEnrichers(t *testing.T) {
	// Clear any existing enrichers
	ClearZapFieldEnrichers()

	// Register multiple enrichers
	RegisterZapFieldEnricher(func(r *http.Request) []zapcore.Field {
		return []zapcore.Field{zap.String("enricher1", "value1")}
	})

	RegisterZapFieldEnricher(func(r *http.Request) []zapcore.Field {
		return []zapcore.Field{zap.String("enricher2", "value2")}
	})

	enrichers := GetZapFieldEnrichers()
	if len(enrichers) != 2 {
		t.Errorf("Expected 2 enrichers, got %d", len(enrichers))
	}

	// Test that all enrichers are called
	req := httptest.NewRequest("GET", "http://example.com/test", nil)

	allFields := make([]zapcore.Field, 0)
	for _, enricher := range enrichers {
		fields := enricher(req)
		allFields = append(allFields, fields...)
	}

	if len(allFields) != 2 {
		t.Errorf("Expected 2 total fields from enrichers, got %d", len(allFields))
	}
}

func TestZapFieldEnricherReturnsNil(t *testing.T) {
	// Clear any existing enrichers
	ClearZapFieldEnrichers()

	// Register an enricher that returns nil
	RegisterZapFieldEnricher(func(r *http.Request) []zapcore.Field {
		return nil
	})

	req := httptest.NewRequest("GET", "http://example.com/test", nil)

	enrichers := GetZapFieldEnrichers()
	fields := enrichers[0](req)

	if fields != nil {
		t.Errorf("Expected nil fields, got %v", fields)
	}
}
