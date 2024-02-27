package connector

import (
	"encoding/json"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
	"net/http"
	"os"
	"time"
)

type Restarter struct {
	doneChan   chan os.Signal
	apiKey     string
	restarting bool
}

func NewRestarter(doneChan chan os.Signal, apiKey string) *Restarter {
	return &Restarter{
		doneChan:   doneChan,
		apiKey:     apiKey,
		restarting: false,
	}
}

// CreateEndpoint Create a new endpoint for the restarter
func (re *Restarter) CreateEndpoint() *chi.Mux {
	router := chi.NewRouter()
	router.Post("/restart", re.restart)
	return router
}

func (re *Restarter) restart(w http.ResponseWriter, r *http.Request) {
	if re.restarting {
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	body := struct {
		apiKey string
	}{}

	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	id := chi.URLParam(r, "id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	zap.L().Info("Received a restart request")

	// permanently setting to true, since the system will restart
	re.restarting = true

	time.AfterFunc(time.Second, func() {
		re.doneChan <- os.Interrupt
	})

	w.WriteHeader(http.StatusOK)
}
