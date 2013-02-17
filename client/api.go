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
)

var rev_error = errors.New("No X-Revision Found")
var http_error = errors.New("Unexpected HTTP Response")

type HiberaClient struct {
	url  string
	lock *sync.Mutex
	http *http.Client
}

func NewHiberaClient(url string) *HiberaClient {
	client := new(HiberaClient)
	client.url = url
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

func getContent(resp *http.Response) (string, error) {
	length := resp.ContentLength
	if length < 0 {
		return "", nil
	}
	buf := make([]byte, length)
	_, err := io.ReadFull(resp.Body, buf)
	if err != nil {
		return "", err
	}
	return string(buf), nil
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

func (h *HiberaClient) Lock(key string, timeout uint, name string) (uint64, error) {
	args := makeArgs(fmt.Sprintf("/locks/%s", key))
	args.params["timeout"] = string(timeout)
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

func (h *HiberaClient) State(key string) (string, uint64, error) {
	args := makeArgs(fmt.Sprintf("/locks/%s", key))
	resp, err := h.doreq("DELETE", args)
	if err != nil {
		return "", 0, err
	}
	if resp.StatusCode != 200 {
		return "", 0, http_error
	}
	state, err := getContent(resp)
	if err != nil {
		return "", 0, err
	}
	rev, err := getRev(resp)
	return state, rev, err
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
	members, err := getContent(resp)
	if err != nil {
		return nil, 0, err
	}
	rev, err := getRev(resp)
	return strings.Split(members, "\n"), rev, err
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
	value, err := getContent(resp)
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
	items, err := getContent(resp)
	if err != nil {
		return nil, err
	}
	return strings.Split(items, "\n"), nil
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
