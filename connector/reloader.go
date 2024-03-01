package connector

import (
	"encoding/json"
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
	apiKey   string
}

func NewReloader(action func(string) error, cooldown time.Duration, apiKey string) *Reloader {
	return &Reloader{
		action:   action,
		cooldown: cooldown,
		apiKey:   apiKey,
	}
}

// BindEndpoint Binds the reload endpoint to a existing router
func (re *Reloader) BindEndpoint(rg chi.Router) {
	rg.Post("/reload/{id}", re.reload)
}

// reload the action, if the cooldown has passed, otherwise return 429
func (re *Reloader) reload(w http.ResponseWriter, r *http.Request) {
	body := struct {
		ApiKey string `json:"key"`
	}{}

	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		zap.L().Error("reloader: could not unmarshall body", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

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
	err = re.action(id)
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
