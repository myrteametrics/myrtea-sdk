package connector

import (
	"bytes"
	"encoding/json"
	"github.com/go-chi/chi/v5"
	"github.com/myrteametrics/myrtea-sdk/v5/expression"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestRestarterRestartReturnsBadRequestForInvalidBody(t *testing.T) {
	doneChan := make(chan os.Signal, 1)
	restarter := NewRestarter(doneChan, "test-api-key")
	router := chi.NewRouter()
	restarter.BindEndpoint(router)

	req := httptest.NewRequest(http.MethodPost, "/restart", bytes.NewBuffer([]byte("invalid body")))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	expression.AssertEqual(t, rec.Code, http.StatusBadRequest)
}

func TestRestarterRestartReturnsTooManyRequestsIfAlreadyRestarting(t *testing.T) {
	doneChan := make(chan os.Signal, 1)
	restarter := NewRestarter(doneChan, "test-api-key")
	router := chi.NewRouter()
	restarter.BindEndpoint(router)

	restarter.restarting = true

	reqBody, _ := json.Marshal(map[string]string{"key": "test-api-key"})
	req := httptest.NewRequest(http.MethodPost, "/restart", bytes.NewBuffer(reqBody))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	expression.AssertEqual(t, rec.Code, http.StatusTooManyRequests)
}

func TestRestarterRestartReturnsOKForSuccessfulRestart(t *testing.T) {
	doneChan := make(chan os.Signal, 1)
	restarter := NewRestarter(doneChan, "test-api-key")
	router := chi.NewRouter()
	restarter.BindEndpoint(router)

	reqBody, _ := json.Marshal(map[string]string{"key": "test-api-key"})
	req := httptest.NewRequest(http.MethodPost, "/restart", bytes.NewBuffer(reqBody))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	expression.AssertEqual(t, rec.Code, http.StatusOK)
}

func TestRestarterRestartSendsSignalAfterDelay(t *testing.T) {
	doneChan := make(chan os.Signal, 1)
	restarter := NewRestarter(doneChan, "test-api-key")
	router := chi.NewRouter()
	restarter.BindEndpoint(router)

	reqBody, _ := json.Marshal(map[string]string{"key": "test-api-key"})
	req := httptest.NewRequest(http.MethodPost, "/restart", bytes.NewBuffer(reqBody))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	select {
	case sig := <-doneChan:
		expression.AssertEqual(t, sig, os.Interrupt)
	case <-time.After(2 * time.Second):
		t.Fatal("Expected signal not received")
	}
}
