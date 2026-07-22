// End-to-end test of the kuri WebDAV frontend: a full gateway (kuri daemon
// + Yugabyte) is started in containers with RIBS_WEBDAV_ENABLED=true, and
// files are written and read back over the WebDAV protocol.
//
// MongoDB is intentionally absent: file *data* must work without it (the
// metadata sink degrades to a no-op), which is also the configuration the
// harness gateway runs with.
package webdav_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/CIDgravity/filecoin-gateway/test"
	"github.com/test-go/testify/require"
)

var harness *test.FgwHarness

func TestMain(m *testing.M) {
	harness = test.NewFgwHarnessWithEnv(map[string]string{
		"RIBS_WEBDAV_ENABLED": "true",
	}, "8077")
	defer harness.Stop()
	os.Exit(m.Run())
}

func davRequest(t *testing.T, method, path string, headers map[string]string, body io.Reader) *http.Response {
	t.Helper()

	req, err := http.NewRequest(method, harness.GetEndpoint("8077")+path, body)
	require.NoError(t, err)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return res
}

func TestWebDAVPropfindRoot(t *testing.T) {
	res := davRequest(t, "PROPFIND", "/", map[string]string{"Depth": "1"}, nil)
	defer res.Body.Close() // nolint:errcheck

	require.Equal(t, http.StatusMultiStatus, res.StatusCode)

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "multistatus")
}

func TestWebDAVWriteReadRoundtrip(t *testing.T) {
	content := []byte("hello from the webdav e2e test\n")

	// user directory, then a file in it
	res := davRequest(t, "MKCOL", "/testuser", nil, nil)
	require.NoError(t, res.Body.Close())
	require.Equal(t, http.StatusCreated, res.StatusCode)

	res = davRequest(t, "PUT", "/testuser/hello.txt", nil, bytes.NewReader(content))
	require.NoError(t, res.Body.Close())
	require.Equal(t, http.StatusCreated, res.StatusCode)

	// read it back
	res = davRequest(t, "GET", "/testuser/hello.txt", nil, nil)
	defer res.Body.Close() // nolint:errcheck
	require.Equal(t, http.StatusOK, res.StatusCode)

	got, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, content, got)

	// and see it in the directory listing
	res = davRequest(t, "PROPFIND", "/testuser", map[string]string{"Depth": "1"}, nil)
	defer res.Body.Close() // nolint:errcheck
	require.Equal(t, http.StatusMultiStatus, res.StatusCode)

	listing, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Contains(t, string(listing), "hello.txt")
}

func TestWebDAVLargerFile(t *testing.T) {
	// large enough to span several blocks in the MFS DAG
	content := bytes.Repeat([]byte("0123456789abcdef"), 64<<10) // 1 MiB

	res := davRequest(t, "MKCOL", "/bulkuser", nil, nil)
	require.NoError(t, res.Body.Close())
	require.Equal(t, http.StatusCreated, res.StatusCode)

	res = davRequest(t, "PUT", "/bulkuser/big.bin", nil, bytes.NewReader(content))
	require.NoError(t, res.Body.Close())
	require.Equal(t, http.StatusCreated, res.StatusCode)

	res = davRequest(t, "GET", "/bulkuser/big.bin", nil, nil)
	defer res.Body.Close() // nolint:errcheck
	require.Equal(t, http.StatusOK, res.StatusCode)

	got, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, len(content), len(got))
	require.True(t, bytes.Equal(content, got), "content mismatch")

	// ranged read
	res = davRequest(t, "GET", "/bulkuser/big.bin", map[string]string{"Range": "bytes=16-31"}, nil)
	defer res.Body.Close() // nolint:errcheck
	require.Equal(t, http.StatusPartialContent, res.StatusCode)

	part, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, content[16:32], part)
}

func TestWebDAVRenameAndDelete(t *testing.T) {
	content := []byte("move me\n")

	res := davRequest(t, "MKCOL", "/moveuser", nil, nil)
	require.NoError(t, res.Body.Close())
	require.Equal(t, http.StatusCreated, res.StatusCode)

	res = davRequest(t, "PUT", "/moveuser/a.txt", nil, bytes.NewReader(content))
	require.NoError(t, res.Body.Close())
	require.Equal(t, http.StatusCreated, res.StatusCode)

	res = davRequest(t, "MOVE", "/moveuser/a.txt", map[string]string{
		"Destination": fmt.Sprintf("%s/moveuser/b.txt", harness.GetEndpoint("8077")),
	}, nil)
	require.NoError(t, res.Body.Close())
	require.Equal(t, http.StatusCreated, res.StatusCode)

	res = davRequest(t, "GET", "/moveuser/b.txt", nil, nil)
	defer res.Body.Close() // nolint:errcheck
	require.Equal(t, http.StatusOK, res.StatusCode)
	got, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, content, got)

	res = davRequest(t, "GET", "/moveuser/a.txt", nil, nil)
	require.NoError(t, res.Body.Close())
	require.Equal(t, http.StatusNotFound, res.StatusCode)

	res = davRequest(t, "DELETE", "/moveuser/b.txt", nil, nil)
	require.NoError(t, res.Body.Close())
	require.True(t, res.StatusCode == http.StatusNoContent || res.StatusCode == http.StatusOK,
		"unexpected DELETE status %d", res.StatusCode)

	res = davRequest(t, "GET", "/moveuser/b.txt", nil, nil)
	require.NoError(t, res.Body.Close())
	require.Equal(t, http.StatusNotFound, res.StatusCode)
}
