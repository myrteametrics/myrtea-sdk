package connector

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/myrteametrics/myrtea-sdk/v5/expression"
)

type runtimeControllableMock struct {
	startFn func(id string) error
	stopFn  func(id string) error
}

func (m runtimeControllableMock) Start(id string) error {
	if m.startFn == nil {
		return nil
	}
	return m.startFn(id)
}

func (m runtimeControllableMock) Stop(id string) error {
	if m.stopFn == nil {
		return nil
	}
	return m.stopFn(id)
}

func TestRuntimeControllerReturnsBadRequestForInvalidBody(t *testing.T) {
	controller := NewRuntimeController(runtimeControllableMock{}, "test-api-key")
	router := chi.NewRouter()
	controller.BindEndpoint(router)

	req := httptest.NewRequest(http.MethodPost, "/runtime/start/component-a", bytes.NewBuffer([]byte("invalid body")))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	expression.AssertEqual(t, rec.Code, http.StatusBadRequest)
}

func TestRuntimeControllerReturnsUnauthorizedForInvalidKey(t *testing.T) {
	controller := NewRuntimeController(runtimeControllableMock{}, "test-api-key")
	router := chi.NewRouter()
	controller.BindEndpoint(router)

	reqBody, _ := json.Marshal(map[string]string{"key": "invalid"})
	req := httptest.NewRequest(http.MethodPost, "/runtime/start/component-a", bytes.NewBuffer(reqBody))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	expression.AssertEqual(t, rec.Code, http.StatusUnauthorized)
}

func TestRuntimeControllerReturnsBadRequestForUnsupportedAction(t *testing.T) {
	controller := NewRuntimeController(runtimeControllableMock{}, "")
	router := chi.NewRouter()
	controller.BindEndpoint(router)

	reqBody, _ := json.Marshal(map[string]string{"key": ""})
	req := httptest.NewRequest(http.MethodPost, "/runtime/pause/component-a", bytes.NewBuffer(reqBody))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	expression.AssertEqual(t, rec.Code, http.StatusBadRequest)
}

func TestRuntimeControllerCanStartAllComponents(t *testing.T) {
	calledWith := ""
	controller := NewRuntimeController(runtimeControllableMock{
		startFn: func(id string) error {
			calledWith = id
			return nil
		},
	}, "")
	router := chi.NewRouter()
	controller.BindEndpoint(router)

	reqBody, _ := json.Marshal(map[string]string{"key": ""})
	req := httptest.NewRequest(http.MethodPost, "/runtime/start", bytes.NewBuffer(reqBody))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	expression.AssertEqual(t, rec.Code, http.StatusOK)
	expression.AssertEqual(t, calledWith, AllRuntimeComponentsID)
}

func TestRuntimeControllerReturnsNotFoundWhenComponentIsUnknown(t *testing.T) {
	controller := NewRuntimeController(runtimeControllableMock{
		stopFn: func(id string) error {
			return RuntimeControllerComponentNotFoundErr
		},
	}, "")
	router := chi.NewRouter()
	controller.BindEndpoint(router)

	reqBody, _ := json.Marshal(map[string]string{"key": ""})
	req := httptest.NewRequest(http.MethodPost, "/runtime/stop/component-a", bytes.NewBuffer(reqBody))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	expression.AssertEqual(t, rec.Code, http.StatusNotFound)
}

func TestRuntimeControllerReturnsInternalServerErrorForUnexpectedErrors(t *testing.T) {
	controller := NewRuntimeController(runtimeControllableMock{
		startFn: func(id string) error {
			return errors.New("boom")
		},
	}, "")
	router := chi.NewRouter()
	controller.BindEndpoint(router)

	reqBody, _ := json.Marshal(map[string]string{"key": ""})
	req := httptest.NewRequest(http.MethodPost, "/runtime/start/component-a", bytes.NewBuffer(reqBody))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	expression.AssertEqual(t, rec.Code, http.StatusInternalServerError)
}

func TestRuntimeManagerCanControlSingleComponentAndAllComponents(t *testing.T) {
	manager := NewRuntimeManager(true)
	manager.Register("topic-a", true)
	manager.Register("topic-b", true)

	expression.AssertEqual(t, manager.Stop("topic-a"), nil)
	expression.AssertEqual(t, manager.IsRunning("topic-a"), false)
	expression.AssertEqual(t, manager.IsRunning("topic-b"), true)

	expression.AssertEqual(t, manager.Stop(AllRuntimeComponentsID), nil)
	expression.AssertEqual(t, manager.IsRunning("topic-a"), false)
	expression.AssertEqual(t, manager.IsRunning("topic-b"), false)

	expression.AssertEqual(t, manager.Start(AllRuntimeComponentsID), nil)
	expression.AssertEqual(t, manager.IsRunning("topic-a"), true)
	expression.AssertEqual(t, manager.IsRunning("topic-b"), true)
}

func TestRuntimeManagerReturnsNotFoundForUnknownComponent(t *testing.T) {
	manager := NewRuntimeManager(true)

	err := manager.Stop("unknown-topic")

	expression.AssertEqual(t, errors.Is(err, RuntimeControllerComponentNotFoundErr), true)
}
