// Package workerclient is the controller's client for the worker chunk API.
package workerclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/Dd1235/objstore/internal/api"
	"github.com/Dd1235/objstore/internal/chunker"
)

var (
	ErrNotFound = errors.New("chunk not found on worker")
	ErrCorrupt  = errors.New("worker reports chunk corrupt")
	ErrConflict = errors.New("worker holds different data under this chunk id")
)

// Client talks to workers over HTTP.
type Client struct {
	HTTP     *http.Client
	MaxChunk int64 // largest chunk body accepted from a worker; default chunker.Size
}

// Put stores a chunk on the worker at baseURL.
func (c *Client) Put(ctx context.Context, baseURL, chunkID string, data []byte, sum [sha256.Size]byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, chunkURL(baseURL, chunkID), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(api.HeaderChunkSHA256, hex.EncodeToString(sum[:]))
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer drain(resp.Body)
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return responseError(baseURL, resp)
	}
	return nil
}

// Get fetches a chunk. The worker has already verified it against its own
// checksum; callers should still check it against the controller's record.
func (c *Client) Get(ctx context.Context, baseURL, chunkID string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, chunkURL(baseURL, chunkID), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer drain(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, responseError(baseURL, resp)
	}
	limit := c.MaxChunk
	if limit <= 0 {
		limit = chunker.Size
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("worker %s: chunk %s larger than %d bytes", baseURL, chunkID, limit)
	}
	return data, nil
}

// Delete removes a chunk. It returns ErrNotFound if the worker did not have it.
func (c *Client) Delete(ctx context.Context, baseURL, chunkID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, chunkURL(baseURL, chunkID), nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer drain(resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		return responseError(baseURL, resp)
	}
	return nil
}

// Inventory lists the chunks a worker holds.
func (c *Client) Inventory(ctx context.Context, baseURL, workerID string) (api.ChunkInventory, error) {
	var inv api.ChunkInventory
	u := strings.TrimSuffix(baseURL, "/") + "/internal/workers/" + url.PathEscape(workerID) + "/chunks"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return inv, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return inv, err
	}
	defer drain(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return inv, responseError(baseURL, resp)
	}
	err = json.NewDecoder(resp.Body).Decode(&inv)
	return inv, err
}

func chunkURL(baseURL, chunkID string) string {
	return strings.TrimSuffix(baseURL, "/") + "/internal/chunks/" + url.PathEscape(chunkID)
}

func responseError(baseURL string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	var e api.Error
	if json.Unmarshal(body, &e) != nil || e.Code == "" {
		return fmt.Errorf("worker %s: %s", baseURL, resp.Status)
	}
	switch e.Code {
	case api.CodeNotFound:
		return ErrNotFound
	case api.CodeChunkCorrupt:
		return ErrCorrupt
	case api.CodeConflict:
		return ErrConflict
	}
	return fmt.Errorf("worker %s: %w", baseURL, &e)
}

func drain(r io.ReadCloser) {
	io.Copy(io.Discard, io.LimitReader(r, 64<<10))
	r.Close()
}
