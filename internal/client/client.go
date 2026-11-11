// Package client is a Go client for the controller's REST API.
package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"

	"github.com/Dd1235/objstore/internal/api"
)

// ErrIntegrity means downloaded bytes did not match the object's size or
// checksum.
var ErrIntegrity = errors.New("downloaded data does not match the object's checksum")

// Client talks to a controller.
type Client struct {
	BaseURL string       // for example http://localhost:8090
	HTTP    *http.Client // nil means http.DefaultClient
}

// Error is an error response from the controller.
type Error struct {
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("HTTP %d", e.Status)
	}
	return e.Code + ": " + e.Message
}

// IsNotFound reports whether err is a 404 from the controller.
func IsNotFound(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == http.StatusNotFound
}

// Upload stores the contents of r as a new object. contentType may be empty
// to let the controller pick one from the name or the content.
func (c *Client) Upload(ctx context.Context, name string, r io.Reader, contentType string, metadata map[string]string) (api.ObjectInfo, error) {
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		pw.CloseWithError(writeUpload(mw, name, r, contentType, metadata))
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url("/api/v1/objects"), pr)
	if err != nil {
		pr.Close()
		return api.ObjectInfo{}, err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	var info api.ObjectInfo
	err = c.do(req, http.StatusCreated, &info)
	pr.Close()
	return info, err
}

func writeUpload(mw *multipart.Writer, name string, r io.Reader, contentType string, metadata map[string]string) error {
	if len(metadata) > 0 {
		body, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		if err := mw.WriteField("metadata", string(body)); err != nil {
			return err
		}
	}
	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": "file", "filename": name}))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	h.Set("Content-Type", contentType)
	fw, err := mw.CreatePart(h)
	if err != nil {
		return err
	}
	if _, err := io.Copy(fw, r); err != nil {
		return err
	}
	return mw.Close()
}

// Download describes a downloaded object.
type Download struct {
	ObjectID    string
	Name        string
	ContentType string
	Size        int64
	SHA256      string
}

// Download writes an object's bytes to w. It checks the byte count and the
// SHA-256 against the object's metadata and returns ErrIntegrity (after
// writing) if they differ; callers writing to a file should discard it then.
func (c *Client) Download(ctx context.Context, id string, w io.Writer) (Download, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url("/api/v1/objects/"+url.PathEscape(id)), nil)
	if err != nil {
		return Download{}, err
	}
	resp, err := c.http().Do(req)
	if err != nil {
		return Download{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Download{}, readError(resp)
	}
	d := Download{
		ObjectID:    resp.Header.Get("X-Object-Id"),
		ContentType: resp.Header.Get("Content-Type"),
		Size:        resp.ContentLength,
		SHA256:      strings.Trim(resp.Header.Get("ETag"), `"`),
	}
	if _, params, err := mime.ParseMediaType(resp.Header.Get("Content-Disposition")); err == nil {
		d.Name = params["filename"]
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(w, h), resp.Body)
	if err != nil {
		return d, fmt.Errorf("download interrupted after %d bytes: %w", n, err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); (d.Size >= 0 && n != d.Size) || got != d.SHA256 {
		return d, fmt.Errorf("%w: got %d bytes with sha256 %s, want %d bytes with %s", ErrIntegrity, n, got, d.Size, d.SHA256)
	}
	return d, nil
}

// Metadata returns an object's metadata, including chunk locations.
func (c *Client) Metadata(ctx context.Context, id string) (api.ObjectInfo, error) {
	var info api.ObjectInfo
	err := c.getJSON(ctx, "/api/v1/objects/"+url.PathEscape(id)+"/metadata", &info)
	return info, err
}

// PatchMetadata changes custom metadata: a non-nil value sets a key, nil
// removes it.
func (c *Client) PatchMetadata(ctx context.Context, id string, patch map[string]*string) (api.ObjectInfo, error) {
	body, err := json.Marshal(patch)
	if err != nil {
		return api.ObjectInfo{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, c.url("/api/v1/objects/"+url.PathEscape(id)+"/metadata"), bytes.NewReader(body))
	if err != nil {
		return api.ObjectInfo{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	var info api.ObjectInfo
	err = c.do(req, http.StatusOK, &info)
	return info, err
}

// List returns a page of objects, oldest first. A limit of 0 uses the
// controller's default.
func (c *Client) List(ctx context.Context, limit, offset int) (api.ObjectList, error) {
	q := url.Values{}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if offset > 0 {
		q.Set("offset", strconv.Itoa(offset))
	}
	path := "/api/v1/objects"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	var list api.ObjectList
	err := c.getJSON(ctx, path, &list)
	return list, err
}

// Delete removes an object.
func (c *Client) Delete(ctx context.Context, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.url("/api/v1/objects/"+url.PathEscape(id)), nil)
	if err != nil {
		return err
	}
	return c.do(req, http.StatusNoContent, nil)
}

// Workers returns the controller's view of every worker.
func (c *Client) Workers(ctx context.Context) (api.WorkerList, error) {
	var list api.WorkerList
	err := c.getJSON(ctx, "/internal/workers", &list)
	return list, err
}

// WorkerChunks returns the chunks a worker stores.
func (c *Client) WorkerChunks(ctx context.Context, workerID string) (api.ChunkInventory, error) {
	var inv api.ChunkInventory
	err := c.getJSON(ctx, "/internal/workers/"+url.PathEscape(workerID)+"/chunks", &inv)
	return inv, err
}

// Replication reports whether every chunk has its required copies.
func (c *Client) Replication(ctx context.Context) (api.ReplicationStatus, error) {
	var st api.ReplicationStatus
	err := c.getJSON(ctx, "/internal/replication", &st)
	return st, err
}

func (c *Client) getJSON(ctx context.Context, path string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url(path), nil)
	if err != nil {
		return err
	}
	return c.do(req, http.StatusOK, v)
}

// do sends req and decodes a JSON response with status want into v.
func (c *Client) do(req *http.Request, want int, v any) error {
	resp, err := c.http().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		return readError(resp)
	}
	if v == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func readError(resp *http.Response) error {
	e := &Error{Status: resp.StatusCode}
	var body api.Error
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&body); err == nil {
		e.Code, e.Message = body.Code, body.Message
	}
	return e
}

func (c *Client) url(path string) string { return strings.TrimRight(c.BaseURL, "/") + path }

func (c *Client) http() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}
