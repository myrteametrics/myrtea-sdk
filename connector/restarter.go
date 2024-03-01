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

// BindEndpoint Binds the restart endpoint to an existing router
func (re *Restarter) BindEndpoint(rg chi.Router) {
	rg.Post("/restart", re.restart)
}

func (re *Restarter) restart(w http.ResponseWriter, r *http.Request) {
	if re.restarting {
		zap.L().Info("Received a restart request, but service is already in a restart state")
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	body := struct {
		ApiKey string `json:"key"`
	}{}

	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		zap.L().Error("restarter: could not unmarshall body", zap.Error(err))
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
