package connector

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/myrteametrics/myrtea-sdk/v5/expression"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

func TestReloaderReloadReturnsBadRequestForInvalidBody(t *testing.T) {
	action := func(id string) error { return nil }
	reloader := NewReloader(action, time.Minute, "test-api-key")
	router := chi.NewRouter()
	reloader.BindEndpoint(router)

	req := httptest.NewRequest(http.MethodPost, "/reload/test-id", bytes.NewBuffer([]byte("invalid body")))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	expression.AssertEqual(t, rec.Code, http.StatusBadRequest)
}

func TestReloaderReloadReturnsTooManyRequestsIfCooldownNotPassed(t *testing.T) {
	action := func(id string) error { return nil }
	reloader := NewReloader(action, time.Minute, "test-api-key")
	router := chi.NewRouter()
	reloader.BindEndpoint(router)

	reloader.last = time.Now()

	reqBody, _ := json.Marshal(map[string]string{"key": "test-api-key"})
	req := httptest.NewRequest(http.MethodPost, "/reload/test-id", bytes.NewBuffer(reqBody))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	expression.AssertEqual(t, rec.Code, http.StatusTooManyRequests)
}

func TestReloaderReloadReturnsOKForSuccessfulAction(t *testing.T) {
	action := func(id string) error { return nil }
	reloader := NewReloader(action, time.Minute, "test-api-key")
	router := chi.NewRouter()
	reloader.BindEndpoint(router)

	reqBody, _ := json.Marshal(map[string]string{"key": "test-api-key"})
	req := httptest.NewRequest(http.MethodPost, "/reload/test-id", bytes.NewBuffer(reqBody))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	expression.AssertEqual(t, rec.Code, http.StatusOK)
}

func TestReloaderReloadReturnsNotFoundForComponentNotFound(t *testing.T) {
	action := func(id string) error { return ReloaderComponentNotFoundErr }
	reloader := NewReloader(action, time.Minute, "test-api-key")
	router := chi.NewRouter()
	reloader.BindEndpoint(router)

	reqBody, _ := json.Marshal(map[string]string{"key": "test-api-key"})
	req := httptest.NewRequest(http.MethodPost, "/reload/test-id", bytes.NewBuffer(reqBody))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	expression.AssertEqual(t, rec.Code, http.StatusNotFound)
}

func TestReloaderReloadReturnsInternalServerErrorForOtherErrors(t *testing.T) {
	action := func(id string) error { return errors.New("some error") }
	reloader := NewReloader(action, time.Minute, "test-api-key")
	router := chi.NewRouter()
	reloader.BindEndpoint(router)

	reqBody, _ := json.Marshal(map[string]string{"key": "test-api-key"})
	req := httptest.NewRequest(http.MethodPost, "/reload/test-id", bytes.NewBuffer(reqBody))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	expression.AssertEqual(t, rec.Code, http.StatusInternalServerError)
}
