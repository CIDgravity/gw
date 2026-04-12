package rbdeal

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalWebPreDealTransferCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodHead, r.Method)
		w.Header().Set("Content-Length", "128")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	lwi := &LocalWebInfo{}
	require.NoError(t, lwi.PreDealTransferCheck(context.Background(), 1, server.URL, 128))
}

func TestLocalWebPreDealTransferCheckRejectsWrongSize(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "64")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	lwi := &LocalWebInfo{}
	err := lwi.PreDealTransferCheck(context.Background(), 1, server.URL, 128)
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("expected %d", 128))
}
