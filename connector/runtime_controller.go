package connector

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

const AllRuntimeComponentsID = "*"

var RuntimeControllerComponentNotFoundErr = errors.New("component not found")

// RuntimeControllable exposes the minimum start/stop contract used by RuntimeController.
type RuntimeControllable interface {
	Start(id string) error
	Stop(id string) error
}

// RuntimeController exposes optional HTTP endpoints to start/stop connector components at runtime.
type RuntimeController struct {
	controller RuntimeControllable
	apiKey     string
}

func NewRuntimeController(controller RuntimeControllable, apiKey string) *RuntimeController {
	return &RuntimeController{controller: controller, apiKey: apiKey}
}

// BindEndpoint binds runtime control endpoints to an existing router.
func (rc *RuntimeController) BindEndpoint(rg chi.Router) {
	rg.Post("/runtime/{action}", rc.control)
	rg.Post("/runtime/{action}/{id}", rc.control)
}

func (rc *RuntimeController) control(w http.ResponseWriter, r *http.Request) {
	if rc.controller == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	body := struct {
		ApiKey string `json:"key"`
	}{}

	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		zap.L().Error("runtime controller: could not unmarshall body", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if rc.apiKey != "" && body.ApiKey != rc.apiKey {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	action := strings.ToLower(chi.URLParam(r, "action"))
	if action != "start" && action != "stop" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	id := chi.URLParam(r, "id")
	if id == "" {
		id = AllRuntimeComponentsID
	}

	if action == "start" {
		err = rc.controller.Start(id)
	} else {
		err = rc.controller.Stop(id)
	}

	if err == nil {
		w.WriteHeader(http.StatusOK)
		return
	}

	if errors.Is(err, RuntimeControllerComponentNotFoundErr) {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	zap.L().Error("runtime controller: operation failed", zap.Error(err), zap.String("action", action), zap.String("id", id))
	w.WriteHeader(http.StatusInternalServerError)
}

// RuntimeManager stores start/stop state for connector components.
// Components can be controlled individually or globally through the * identifier.
type RuntimeManager struct {
	mu             sync.RWMutex
	defaultRunning bool
	states         map[string]bool
}

func NewRuntimeManager(defaultRunning bool) *RuntimeManager {
	return &RuntimeManager{
		defaultRunning: defaultRunning,
		states:         make(map[string]bool),
	}
}

func (rm *RuntimeManager) Register(id string, running bool) {
	if id == "" || id == AllRuntimeComponentsID {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.states[id] = running
}

func (rm *RuntimeManager) Start(id string) error {
	return rm.setRunning(id, true)
}

func (rm *RuntimeManager) Stop(id string) error {
	return rm.setRunning(id, false)
}

func (rm *RuntimeManager) IsRunning(id string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if state, ok := rm.states[id]; ok {
		return state
	}

	return rm.defaultRunning
}

func (rm *RuntimeManager) setRunning(id string, running bool) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if id == AllRuntimeComponentsID {
		rm.defaultRunning = running
		for componentID := range rm.states {
			rm.states[componentID] = running
		}
		return nil
	}

	if _, ok := rm.states[id]; !ok {
		return RuntimeControllerComponentNotFoundErr
	}

	rm.states[id] = running
	return nil
}
