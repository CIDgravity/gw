package rbdeal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/mitchellh/go-homedir"

	types "github.com/CIDgravity/filecoin-gateway/ributil/boosttypes"
	"github.com/google/uuid"
	"golang.org/x/crypto/acme/autocert"
	"golang.org/x/xerrors"
)

var bootTime = time.Now()

type countingResponseWriter struct {
	http.ResponseWriter
	total *atomic.Int64
}

func (cw *countingResponseWriter) Write(p []byte) (int, error) {
	n, err := cw.ResponseWriter.Write(p)
	if n > 0 && cw.total != nil {
		cw.total.Add(int64(n))
	}
	return n, err
}

func (r *ribs) setupCarServer(ctx context.Context) error {
	cfg := configuration.GetConfig()
	if !cfg.External.Localweb.BuiltinServer {
		return nil
	}

	handler := http.NewServeMux()
	handler.HandleFunc("/", r.handleCarRequest)
	server := &http.Server{
		Handler: handler, // todo gzip handler assuming that it works with boost
		// This context will be the parent of the context associated with all
		// incoming requests
		BaseContext: func(listener net.Listener) context.Context {
			return ctx
		},
	}

	if cfg.External.Localweb.ServerTLS {
		repoDir := cfg.Ribs.DataDir
		repoDir, err := homedir.Expand(repoDir)
		if err != nil {
			return xerrors.Errorf("failed to expand repo dir: %w", err)
		}

		if err := os.MkdirAll(repoDir, 0755); err != nil {
			return xerrors.Errorf("failed to create repo dir: %w", err)
		}

		// tls uses letsencrypt autocert and gets the domain from Url
		parsedURL, err := url.Parse(cfg.External.Localweb.Url)
		if err != nil {
			return xerrors.Errorf("failed to parse EXTERNAL_LOCALWEB_URL: %w", err)
		}
		certManager := autocert.Manager{
			Prompt:     autocert.AcceptTOS,
			HostPolicy: autocert.HostWhitelist(parsedURL.Hostname()),
			Cache:      autocert.DirCache(filepath.Join(repoDir, "acme")),
		}

		server.TLSConfig = certManager.TLSConfig()
	}

	listenPort, err := strconv.Atoi(cfg.External.Localweb.ServerPort)
	if err != nil {
		return xerrors.Errorf("failed to convert server port to int: %w", err)
	}

	// always listen on 0.0.0.0
	server.Addr = fmt.Sprintf("0.0.0.0:%d", listenPort)

	go func() {
		if cfg.External.Localweb.ServerTLS {
			server.ListenAndServeTLS("", "")
		} else {
			server.ListenAndServe()
		}
	}()

	//go r.carStatsWorker(ctx)

	return nil
}

/* func (r *ribs) carStatsWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Millisecond * 250):
			r.updateCarStats()
		}
	}
} */
/*
func (r *ribs) updateCarStats() {
	r.uploadStatsLk.Lock()
	defer r.uploadStatsLk.Unlock()

	r.uploadStatsSnap = make(map[iface.GroupKey]*iface.GroupUploadStats)
	for k, v := range r.uploadStats {
		r.uploadStatsSnap[k] = &iface.GroupUploadStats{
			ActiveRequests: v.ActiveRequests,
			UploadBytes:    atomic.LoadInt64(&v.UploadBytes),
		}
	}

	for k, v := range r.uploadStats {
		if v.ActiveRequests == 0 {
			delete(r.uploadStats, k)
		}
	}
}

func (r *ribs) CarUploadStats() iface.UploadStats {
	lastTotalBytes, err := r.db.LastTotalUploadedBytes()
	if err != nil {
		log.Errorw("getting last total uploaded bytes", "error", err)
	}

	r.uploadStatsLk.Lock()
	defer r.uploadStatsLk.Unlock()

	return iface.UploadStats{
		ByGroup:        r.uploadStatsSnap,
		LastTotalBytes: lastTotalBytes,
	}
} */

func (r *ribs) makeCarRequest(group int64, timeout time.Duration, carSize int64, deal uuid.UUID) (types.Transfer, error) {
	cfg := configuration.GetConfig()

	if cfg.External.S3.Endpoint != "" {
		return types.Transfer{}, xerrors.Errorf("s3 endpoint is set, direct to s3 TODO")
	}

	// external offload
	extu, err := r.maybeGetExternalURL(group)
	if err != nil {
		return types.Transfer{}, xerrors.Errorf("XYZ: car request: external url: %w", err)
	}
	if extu == nil {
		return types.Transfer{}, xerrors.Errorf("XYZ: car request: external url is nil")
	}

	transferParams := &types.HttpRequest{URL: *extu}

	paramsBytes, err := json.Marshal(transferParams)
	if err != nil {
		return types.Transfer{}, fmt.Errorf("marshalling request parameters: %w", err)
	}

	transfer := types.Transfer{
		Type:   "http",
		Params: paramsBytes,
		Size:   uint64(carSize),
	}

	return transfer, nil
}

func (r *ribs) handleCarRequest(w http.ResponseWriter, req *http.Request) {
	requestPath := strings.TrimPrefix(pathpkg.Clean("/"+req.URL.Path), "/")
	if requestPath == "" || requestPath == "." || strings.Contains(requestPath, "/") {
		log.Warnw("car request: invalid path", "url", req.URL)
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}

	group, err := r.db.GetGroupByExternalPath(EXTERNAL_LOCALWEB, requestPath)
	if err != nil {
		log.Errorw("car request: lookup external path", "error", err, "path", requestPath, "url", req.URL)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if group == nil {
		log.Warnw("car request: unknown path", "path", requestPath, "url", req.URL)
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	log := log.With("group", *group, "path", requestPath)
	r.carUploadActive.Add(1)
	defer r.carUploadActive.Add(-1)
	w.Header().Set("Content-Type", "application/vnd.ipld.car")

	cf, err := r.externalOffloader.ReadCarFile(req.Context(), *group)
	if err != nil {
		log.Errorw("car request: read car file", "error", err, "url", req.URL, "group", *group, "remote", req.RemoteAddr)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	defer cf.Close()
	cw := &countingResponseWriter{ResponseWriter: w, total: &r.carUploadBytes}
	http.ServeContent(cw, req, "gdata.car", time.Time{}, cf)
	if err != nil {
		log.Errorw("car request: write car", "error", err, "url", req.URL, "group", *group, "remote", req.RemoteAddr)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type LimitWriter struct {
	W       io.Writer
	N       int64
	Err     error
	Reached bool // flag to indicate whether the limit has been reached
}

func (lw *LimitWriter) Write(p []byte) (n int, err error) {
	if lw.Reached {
		return 0, lw.Err
	}
	if lw.N <= 0 {
		lw.Reached = true
		return 0, lw.Err
	}
	if int64(len(p)) > lw.N {
		p = p[0:lw.N]
		lw.Reached = true
	}
	n, err = lw.W.Write(p)
	lw.N -= int64(n)
	if lw.N <= 0 {
		lw.Reached = true
		lw.Err = err
	}
	return
}
