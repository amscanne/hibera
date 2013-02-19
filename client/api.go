package client

import (
	"bytes"
	"strconv"
	"errors"
	"fmt"
	"io"
	"os"
	"log"
	"strings"
	"time"
	"crypto/tls"
	"net/url"
	"net/http"
	"math/rand"
	"encoding/json"
	"hibera/core"
	"hibera/storage"
)

var NoRevision = errors.New("No X-Revision Found")
var DefaultPort = uint(2033)
var DefaultHost = "localhost"

type HiberaAPI struct {
	urls     []string
	clientid string
	delay    uint
	*http.Client
}

func generateURLs(addrs string) []string {
	raw := strings.Split(addrs, ",")
	urls := make([]string, len(raw), len(raw))
	for i, addr := range raw {
		idx := strings.Index(addr, ":")
		port := DefaultPort
		if idx >= 0 && idx+1 < len(addr) {
			parsed, err := strconv.ParseUint(addr[idx+1:], 0, 32)
			if err != nil {
				port = DefaultPort
			} else {
				port = uint(parsed)
			}
			addr = addr[0:idx]
		}
		if len(addr) == 0 {
			addr = DefaultHost
		}
		urls[i] = fmt.Sprintf("http://%s:%d", addr, port)
	}
	return urls
}

func generateClientId() string {
	// Check the environment for an existing Id.
	clientid := os.Getenv("HIBERA_CLIENT_ID")
	if clientid == "" {
		var err error
		clientid, err = storage.Uuid()
		if err != nil {
			return ""
		}

		// Save for other connections within
		// this process and for any subprocesses.
		// This matches expected semantics. If
		// you want more control, you can use the
		// HiberaAPI class.
		os.Setenv("HIBERA_CLIENT_ID", clientid)
	}
	return clientid
}

func NewHiberaAPI(urls []string, clientid string, delay uint) *HiberaAPI {
	// Check the clientId.
	if clientid == "" {
		return nil
	}

	// Allocate our client.
	api := new(HiberaAPI)
	api.urls = urls
	api.clientid = clientid
	api.delay = delay

	// Create our HTTP transport.
	// Nothing really special about this, but we may want
	// to tune parameters for idling connections, etc.
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	api.Client = &http.Client{Transport: tr}

	return api
}

func NewHiberaClient(addrs string, delay uint) *HiberaAPI {
	urls := generateURLs(addrs)
	clientid := generateClientId()
	return NewHiberaAPI(urls, clientid, delay)
}

// This object is used to serialize
// information about members to JSON.
// The server-side has it's own type,
// we don't share across.
type syncInfo struct {
	Index   int
	Members []string
}

type HttpArgs struct {
	path    string
	headers map[string]string
	params  map[string]string
	body    io.Reader
}

func (h *HiberaAPI) makeArgs(path string) HttpArgs {
	headers := make(map[string]string)
	params := make(map[string]string)
	return HttpArgs{path, headers, params, nil}
}

func (h *HiberaAPI) getRev(resp *http.Response) (uint64, error) {
	rev := resp.Header["X-Revision"]
	if len(rev) == 0 {
		return 0, NoRevision
	}
	return strconv.ParseUint(rev[0], 0, 64)
}

func (h *HiberaAPI) getContent(resp *http.Response) ([]byte, error) {
	length := resp.ContentLength
	if length < 0 {
		return nil, nil
	}
	buf := make([]byte, length)
	_, err := io.ReadFull(resp.Body, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (h *HiberaAPI) makeRequest(method string, args HttpArgs) (*http.Request, error) {
	// Select a random host to make a request.
	addr := new(bytes.Buffer)
	addr.WriteString(h.urls[rand.Int()%len(h.urls)])
	addr.WriteString(args.path)

	// Append all params.
	written := 0
	for key, value := range args.params {
		if written == 0 {
			addr.WriteString("?")
		} else {
			addr.WriteString("&")
		}
		addr.WriteString(url.QueryEscape(key))
		addr.WriteString("=")
		addr.WriteString(url.QueryEscape(value))
		written += 1
	}
	req, err := http.NewRequest(method, addr.String(), args.body)
	if err != nil {
		return req, err
	}

	// Append headers.
	req.Header.Add("X-Client-Id", h.clientid)
	for key, value := range args.headers {
		req.Header.Add(key, value)
	}

	return req, nil
}

func (h *HiberaAPI) doRequest(method string, args HttpArgs) (*http.Response, error) {
	for {
		// Try the actual request.
		var resp *http.Response
		req, err := h.makeRequest(method, args)
		if err == nil {
			resp, err = h.Client.Do(req)
			if err == nil {
				return resp, nil
			}
		}
		if h.delay == 0 {
			return resp, err
		}

		// Print a message to the console and retry.
		log.Printf("%s (delaying %d milliseconds)", err, h.delay)
		time.Sleep(time.Duration(h.delay) * time.Millisecond)
	}
	return nil, nil
}

func (h *HiberaAPI) Info(base uint) (*core.Info, error) {
	args := h.makeArgs("/")
	args.params["base"] = strconv.FormatUint(uint64(base), 10)
	resp, err := h.doRequest("GET", args)
	if err != nil {
		return nil, err
	}
	content, err := h.getContent(resp)
	if err != nil {
		return nil, err
	}
	var info core.Info
	err = json.Unmarshal(content, &info)
	if err != nil {
		return nil, err
	}
	return &info, err
}

func (h *HiberaAPI) Wait(key string, rev uint64, timeout uint) (uint64, error) {
	args := h.makeArgs(fmt.Sprintf("/event/%s", key))
	args.params["rev"] = strconv.FormatUint(rev, 10)
	args.params["timeout"] = strconv.FormatUint(uint64(timeout), 10)
	resp, err := h.doRequest("GET", args)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, errors.New(resp.Status)
	}
	rev, err = h.getRev(resp)
	return rev, err
}

func (h *HiberaAPI) Join(key string, name string, limit uint, timeout uint) (int, uint64, error) {
	args := h.makeArgs(fmt.Sprintf("/sync/%s", key))
	args.params["name"] = name
	args.params["limit"] = strconv.FormatUint(uint64(limit), 10)
	args.params["timeout"] = strconv.FormatUint(uint64(timeout), 10)
	resp, err := h.doRequest("POST", args)
	if err != nil {
		return -1, 0, err
	}
	if resp.StatusCode != 200 {
		return -1, 0, errors.New(resp.Status)
	}
	content, err := h.getContent(resp)
	if err != nil {
		return -1, 0, err
	}
	var index int
	err = json.Unmarshal(content, &index)
	if err != nil {
		return -1, 0, err
	}
	rev, err := h.getRev(resp)
	return index, rev, err
}

func (h *HiberaAPI) Leave(key string, name string) (uint64, error) {
	args := h.makeArgs(fmt.Sprintf("/sync/%s", key))
	args.params["name"] = name
	resp, err := h.doRequest("DELETE", args)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, errors.New(resp.Status)
	}
	rev, err := h.getRev(resp)
	return rev, err
}

func (h *HiberaAPI) Members(key string, name string, limit uint) (int, []string, uint64, error) {
	args := h.makeArgs(fmt.Sprintf("/sync/%s", key))
	args.params["name"] = name
	args.params["limit"] = strconv.FormatUint(uint64(limit), 10)
	resp, err := h.doRequest("GET", args)
	if err != nil {
		return -1, nil, 0, err
	}
	if resp.StatusCode != 200 {
		return -1, nil, 0, errors.New(resp.Status)
	}
	content, err := h.getContent(resp)
	if err != nil {
		return -1, nil, 0, err
	}
	var info syncInfo
	err = json.Unmarshal(content, &info)
	if err != nil {
		return -1, nil, 0, err
	}
	rev, err := h.getRev(resp)
	return info.Index, info.Members, rev, err
}

func (h *HiberaAPI) Get(key string) ([]byte, uint64, error) {
	args := h.makeArgs(fmt.Sprintf("/data/%s", key))
	resp, err := h.doRequest("GET", args)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != 200 {
		return nil, 0, errors.New(resp.Status)
	}
	content, err := h.getContent(resp)
	if err != nil {
		return nil, 0, err
	}
	rev, err := h.getRev(resp)
	return content, rev, err
}

func (h *HiberaAPI) List() ([]string, error) {
	args := h.makeArgs("/data")
	resp, err := h.doRequest("GET", args)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, errors.New(resp.Status)
	}
	content, err := h.getContent(resp)
	if err != nil {
		return nil, err
	}
	var items []string
	err = json.Unmarshal(content, &items)
	if err != nil {
		return nil, err
	}
	return items, nil
}

func (h *HiberaAPI) Set(key string, value []byte, rev uint64) (uint64, error) {
	args := h.makeArgs(fmt.Sprintf("/data/%s", key))
	args.params["rev"] = strconv.FormatUint(rev, 10)
	args.body = bytes.NewBuffer(value)
	resp, err := h.doRequest("POST", args)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, errors.New(resp.Status)
	}
	rev, err = h.getRev(resp)
	return rev, err
}

func (h *HiberaAPI) Remove(key string, rev uint64) (uint64, error) {
	args := h.makeArgs(fmt.Sprintf("/data/%s", key))
	args.params["rev"] = strconv.FormatUint(rev, 10)
	resp, err := h.doRequest("DELETE", args)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, errors.New(resp.Status)
	}
	rev, err = h.getRev(resp)
	return rev, err
}

func (h *HiberaAPI) Clear() error {
	args := h.makeArgs("/data")
	resp, err := h.doRequest("DELETE", args)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New(resp.Status)
	}
	return nil
}

func (h *HiberaAPI) Fire(key string, rev uint64) (uint64, error) {
	args := h.makeArgs(fmt.Sprintf("/event/%s", key))
	args.params["rev"] = strconv.FormatUint(rev, 10)
	resp, err := h.doRequest("POST", args)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, errors.New(resp.Status)
	}
	rev, err = h.getRev(resp)
	return rev, err
}
