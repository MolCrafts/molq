package rpc

import (
	"crypto/subtle"
	"io"
	"net/http"
	"net/url"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/serve"
)

// Handler is the authenticated HTTP entry for the same dispatcher as stdio.
func Handler(service *serve.Service, token string) (http.Handler, error) {
	if len(token) < 16 {
		return nil, protocol.Fail("INVALID_INPUT", "Network serve requires a token of at least 16 characters", nil)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			parsed, err := url.Parse(origin)
			if err != nil || parsed.Host != r.Host {
				writeJSON(w, http.StatusForbidden, errorResponse(nil, protocol.Fail("PERMISSION_DENIED", "Cross-origin serve access denied", nil), -32000))
				return
			}
		}
		if r.URL.Path != "/rpc" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Upgrade") == "websocket" {
			acceptSocket(w, r, service, token)
			return
		}
		auth := r.Header.Get("Authorization")
		expected := "Bearer " + token
		if subtle.ConstantTimeCompare([]byte(auth), []byte(expected)) != 1 {
			writeJSON(w, http.StatusUnauthorized, errorResponse(nil, protocol.Fail("AUTH_REQUIRED", "Serve bearer token required", nil), -32000))
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, maxFrame+1))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errorResponse(nil, protocol.Fail("INVALID_INPUT", "Malformed JSON frame", nil), -32700))
			return
		}
		payload, err := decode(body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errorResponse(nil, asError(err), -32700))
			return
		}
		response := Dispatch(service, payload, false)
		if response == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeJSON(w, http.StatusOK, response)
	}), nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	encoded, err := marshal(value)
	if err != nil {
		http.Error(w, "encode failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
}
