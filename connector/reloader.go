package connector

import (
	"github.com/go-chi/chi/v5"
	"net/http"
	"time"
)

type Reloader struct {
	action   func()
	last     time.Time
	cooldown time.Duration
}

// NewReloader Reload the action
func NewReloader(action func(), cooldown time.Duration) *Reloader {
	return &Reloader{
		action:   action,
		cooldown: cooldown,
	}
}

// CreateEndpoint Create a new endpoint for the reloader
func (re *Reloader) CreateEndpoint() *chi.Mux {
	router := chi.NewRouter()
	router.Get("/reload", re.reload)
	return router
}

// reload the action, if the cooldown has passed, otherwise return 429
func (re *Reloader) reload(w http.ResponseWriter, r *http.Request) {
	if time.Since(re.last) < re.cooldown {
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	re.last = time.Now()
	re.action()

	w.WriteHeader(http.StatusOK)
}
