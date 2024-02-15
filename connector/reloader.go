package connector

import (
	"errors"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
	"net/http"
	"time"
)

var ReloaderComponentNotFoundErr = errors.New("component not found")

type Reloader struct {
	action   func(string) error
	last     time.Time
	cooldown time.Duration
}

// NewReloader Reload the action
func NewReloader(action func(string) error, cooldown time.Duration) *Reloader {
	return &Reloader{
		action:   action,
		cooldown: cooldown,
	}
}

// CreateEndpoint Create a new endpoint for the reloader
func (re *Reloader) CreateEndpoint() *chi.Mux {
	router := chi.NewRouter()
	router.Get("/reload/{id}", re.reload)
	return router
}

// reload the action, if the cooldown has passed, otherwise return 429
func (re *Reloader) reload(w http.ResponseWriter, r *http.Request) {
	if time.Since(re.last) < re.cooldown {
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	id := chi.URLParam(r, "id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	re.last = time.Now()
	err := re.action(id)
	if err == nil {
		w.WriteHeader(http.StatusOK)
		return
	}

	if errors.Is(err, ReloaderComponentNotFoundErr) {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	zap.L().Error("Error reloading", zap.Error(err))
	w.WriteHeader(http.StatusInternalServerError)
}
