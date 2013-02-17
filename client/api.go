package client

import (
	"sync"
	"bytes"
	"strconv"
	"errors"
	"fmt"
	"io"
	"strings"
	"crypto/tls"
	"net/url"
	"net/http"
	"encoding/json"
	"hibera/core"
	"hibera/server"
)

var rev_error = errors.New("No X-Revision Found")
var http_error = errors.New("Unexpected HTTP Response")

type HiberaClient struct {
	url  string
	lock *sync.Mutex
	http *http.Client
}

func NewHiberaClient(addr string) *HiberaClient {
	client := new(HiberaClient)
	idx := strings.Index(addr, ":")
	port := server.DEFAULT_PORT
	if idx >= 0 && idx+1 < len(addr) {
		parsed_port, err := strconv.ParseUint(addr[idx+1:], 0, 32)
		if err != nil {
			port = server.DEFAULT_PORT
		} else {
			port = uint(parsed_port)
		}
		addr = addr[0:idx]
	}
	if len(addr) == 0 {
		addr = "localhost"
	}
	client.url = fmt.Sprintf("http://%s:%d", addr, port)
	client.lock = new(sync.Mutex)
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client.http = &http.Client{Transport: tr}
	return client
}

type HttpArgs struct {
	path    string
	headers map[string]string
	params  map[string]string
}

func makeArgs(path string) HttpArgs {
	headers := make(map[string]string)
	params := make(map[string]string)
	return HttpArgs{path: path, headers: headers, params: params}
}

func getRev(resp *http.Response) (uint64, error) {
	rev := resp.Header["X-Revision"]
	if len(rev) == 0 {
		return 0, rev_error
	}
	return strconv.ParseUint(rev[0], 0, 64)
}

func getContent(resp *http.Response) ([]byte, error) {
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

func (h *HiberaClient) req(method string, args HttpArgs) (*http.Request, error) {
	addr := new(bytes.Buffer)
	addr.WriteString(h.url)
	addr.WriteString(args.path)
	if len(args.params) > 0 {
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
		}
	}
	req, err := http.NewRequest(method, addr.String(), nil)
	if err != nil {
		return req, err
	}
	for key, value := range args.headers {
		req.Header.Add(key, value)
	}
	return req, nil
}

func (h *HiberaClient) doreq(method string, args HttpArgs) (*http.Response, error) {
	req, err := h.req(method, args)
	if err != nil {
		return nil, err
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	return h.http.Do(req)
}

func (h *HiberaClient) Info(base uint) (*core.Info, error) {
	args := makeArgs("/")
	args.params["base"] = string(base)
	resp, err := h.doreq("GET", args)
	if err != nil {
		return nil, err
	}
	content, err := getContent(resp)
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

func (h *HiberaClient) Lock(key string, timeout uint, name string, limit uint) (uint64, error) {
	args := makeArgs(fmt.Sprintf("/locks/%s", key))
	args.params["timeout"] = string(timeout)
	args.params["name"] = string(name)
	args.params["limit"] = string(limit)
	resp, err := h.doreq("POST", args)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, http_error
	}
	rev, err := getRev(resp)
	return rev, err
}

func (h *HiberaClient) Unlock(key string) (uint64, error) {
	args := makeArgs(fmt.Sprintf("/locks/%s", key))
	resp, err := h.doreq("DELETE", args)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, http_error
	}
	rev, err := getRev(resp)
	return rev, err
}

func (h *HiberaClient) Owners(key string) ([]string, uint64, error) {
	args := makeArgs(fmt.Sprintf("/locks/%s", key))
	resp, err := h.doreq("DELETE", args)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != 200 {
		return nil, 0, http_error
	}
	content, err := getContent(resp)
	if err != nil {
		return nil, 0, err
	}
	var owners []string
	err = json.Unmarshal(content, &owners)
	if err != nil {
		return nil, 0, err
	}
	rev, err := getRev(resp)
	return owners, rev, err
}

func (h *HiberaClient) Watch(key string, rev uint64) (uint64, error) {
	args := makeArgs(fmt.Sprintf("/watches/%s", key))
	args.params["rev"] = string(rev)
	resp, err := h.doreq("GET", args)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, http_error
	}
	rev, err = getRev(resp)
	return rev, err
}

func (h *HiberaClient) Join(group string, name string) (uint64, error) {
	args := makeArgs(fmt.Sprintf("/groups/%s/%s", group, name))
	args.params["name"] = string(name)
	resp, err := h.doreq("POST", args)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, http_error
	}
	rev, err := getRev(resp)
	return rev, err
}

func (h *HiberaClient) Leave(group string, name string) (uint64, error) {
	args := makeArgs(fmt.Sprintf("/groups/%s/%s", group, name))
	args.params["name"] = string(name)
	resp, err := h.doreq("DELETE", args)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, http_error
	}
	rev, err := getRev(resp)
	return rev, err
}

func (h *HiberaClient) Members(group string, name string, limit uint) ([]string, uint64, error) {
	args := makeArgs(fmt.Sprintf("/groups/%s", group))
	args.params["limit"] = string(limit)
	args.params["name"] = string(name)
	resp, err := h.doreq("GET", args)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != 200 {
		return nil, 0, http_error
	}
	content, err := getContent(resp)
	if err != nil {
		return nil, 0, err
	}
	var members []string
	err = json.Unmarshal(content, &members)
	if err != nil {
		return nil, 0, err
	}
	rev, err := getRev(resp)
	return members, rev, err
}

func (h *HiberaClient) Get(key string) (string, uint64, error) {
	args := makeArgs(fmt.Sprintf("/data/%s", key))
	resp, err := h.doreq("GET", args)
	if err != nil {
		return "", 0, err
	}
	if resp.StatusCode != 200 {
		return "", 0, http_error
	}
	content, err := getContent(resp)
	if err != nil {
		return "", 0, err
	}
	var value string
	err = json.Unmarshal(content, &value)
	if err != nil {
		return "", 0, err
	}
	rev, err := getRev(resp)
	return value, rev, err
}

func (h *HiberaClient) List() ([]string, error) {
	args := makeArgs("/data/")
	resp, err := h.doreq("GET", args)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, http_error
	}
	content, err := getContent(resp)
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

func (h *HiberaClient) Set(key string, value string, rev uint64) (uint64, error) {
	args := makeArgs(fmt.Sprintf("/data/%s", key))
	args.params["rev"] = string(rev)
	resp, err := h.doreq("POST", args)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, http_error
	}
	rev, err = getRev(resp)
	return rev, err
}

func (h *HiberaClient) Remove(key string, rev uint64) (uint64, error) {
	args := makeArgs(fmt.Sprintf("/data/%s", key))
	args.params["rev"] = string(rev)
	resp, err := h.doreq("DELETE", args)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, http_error
	}
	rev, err = getRev(resp)
	return rev, err
}

func (h *HiberaClient) Clear() error {
	args := makeArgs("/data/")
	resp, err := h.doreq("DELETE", args)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return http_error
	}
	return nil
}

func (h *HiberaClient) Fire(key string, rev uint64) (uint64, error) {
	args := makeArgs(fmt.Sprintf("/watches/%s", key))
	args.params["rev"] = string(rev)
	resp, err := h.doreq("POST", args)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != 200 {
		return 0, http_error
	}
	rev, err = getRev(resp)
	return rev, err
}
