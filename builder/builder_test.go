package builder

import (
	"strings"
	"testing"
	"time"
)

func TestContextualizeDatePlaceholdersTimezone(t *testing.T) {
	now := time.Date(2019, time.August, 19, 12, 30, 0, 0, time.FixedZone("UTC+02", 2*60*60))
	now, _ = time.Parse("2006-01-02T15:04:05.000-07:00", "2019-08-19T12:30:00.000+02:00")

	query := `__now__`
	query = ContextualizeDatePlaceholders(query, now)
	if query != `2019-08-19T12:30:00.000` {
		t.Error("invalid now")
		t.Log(query)
	}

	query = `__begin__`
	query = ContextualizeDatePlaceholders(query, now)
	if query != `2019-08-19T00:00:00.000` {
		t.Error("invalid begin")
		t.Log(query)
	}

	query = `__timezone__`
	query = ContextualizeTimeZonePlaceholders(query, now)
	if query != `+02:00` {
		t.Error("invalid timezone")
		t.Log(query)
	}
}

func TestContextualizeDatePlaceholders(t *testing.T) {
	query := `
		{
			"query": {
				"bool": {
				"must": [
					{
					"range": {
						"in_timestamp": {
						"from": "__begin__",
						"to": "__now__",
						"timezone": "__timezone__"
					}
					}
				]
				}
			}
		}`
	query = ContextualizeDatePlaceholders(query, time.Now().UTC())
	if strings.Contains(query, "__begin__") {
		t.Error("Date contextualization should replace placeholder begin")
	}
	if strings.Contains(query, "__now__") {
		t.Error("Date contextualization should replace placeholder now")
	}
	query = ContextualizeTimeZonePlaceholders(query, time.Now().UTC())
	if strings.Contains(query, "__timezone__") {
		t.Error("Date contextualization should replace placeholder timezone")
	}
}

func TestContextualizePlaceholders(t *testing.T) {
	query := `
		{
			"query": {
				"bool": {
				"must": [
					{
					"term": {
						"name": {
						"value": "__name__"
						}
					}
					}
				]
				}
			}
		}`
	filters := map[string]string{
		"name": "Myrtea Metrics",
	}
	query = ContextualizePlaceholders(query, filters)
	if strings.Contains(query, "__name__") {
		t.Error("Date contextualization should replace placeholder __name__")
	}
}
