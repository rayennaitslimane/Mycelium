package wire

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	HTTP *http.Client
}

func NewClient(timeout time.Duration) *Client {
	return &Client{HTTP: &http.Client{Timeout: timeout}}
}

// DoJSON sends in as JSON (nil = no body) and decodes a 2xx reply into out (nil = discard).
// A non-2xx status is not an error: err is set only for transport and encoding failures.
func (c *Client) DoJSON(ctx context.Context, method, url string, in, out any) (status int, err error) {
	var body []byte
	h := http.Header{}
	if in != nil {
		if body, err = json.Marshal(in); err != nil {
			return 0, err
		}
		h.Set("Content-Type", "application/json")
	}
	status, _, rb, err := c.DoRaw(ctx, method, url, h, body)
	if err != nil {
		return 0, err
	}
	if status/100 == 2 && out != nil && len(rb) > 0 {
		if err := json.Unmarshal(rb, out); err != nil {
			return status, fmt.Errorf("%s %s: decode reply: %w", method, url, err)
		}
	}
	return status, nil
}

// DoRaw sends body with headers h and returns the raw reply, whatever its status.
func (c *Client) DoRaw(ctx context.Context, method, url string, h http.Header, body []byte) (status int, rh http.Header, rb []byte, err error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, r)
	if err != nil {
		return 0, nil, nil, err
	}
	for k, vs := range h {
		req.Header[k] = vs
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	if rb, err = io.ReadAll(resp.Body); err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Header, rb, nil
}

// WriteJSON writes status and, if v is non-nil, v as JSON.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	if v == nil {
		w.WriteHeader(status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func ReadJSON(r *http.Request, v any) error {
	return json.NewDecoder(r.Body).Decode(v)
}
