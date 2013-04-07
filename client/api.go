package client

import (
    "bytes"
    "strconv"
    "errors"
    "fmt"
    "io"
    "os"
    "time"
    "crypto/tls"
    "net/http"
    "net/url"
    "math/rand"
    "encoding/json"
    "hibera/utils"
)

var NoRevision = errors.New("No X-Revision Found")
var DefaultPort = uint(2033)
var DefaultHost = "localhost"

type HiberaAPI struct {
    urls     []string
    auth     string
    clientid string
    delay    uint
    *http.Client
}

func generateClientId() string {
    // Check the environment for an existing Id.
    clientid := os.Getenv("HIBERA_CLIENT_ID")
    if clientid == "" {
        var err error
        clientid, err = utils.Uuid()
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
    utils.Print("CLIENT", "ID %s", clientid)
    return clientid
}

// Reference below to simply get control over redirects.
var skipRedirect = errors.New("")
func noRedirect(req *http.Request, via []*http.Request) error {
    return skipRedirect
}

func NewHiberaAPI(urls []string, auth string, clientid string, delay uint) *HiberaAPI {
    // Check the clientId.
    if clientid == "" {
        return nil
    }

    // Allocate our client.
    api := new(HiberaAPI)
    api.urls = urls
    api.auth = auth
    api.clientid = clientid
    api.delay = delay

    for _, url := range urls {
        utils.Print("CLIENT", "API %s", url)
    }

    // Create our HTTP transport.
    // Nothing really special about this, but we may want
    // to tune parameters for idling connections, etc.
    tr := &http.Transport{
        TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
    }
    api.Client = &http.Client{Transport: tr, CheckRedirect: noRedirect}

    return api
}

func NewHiberaClient(addrs string, auth string, delay uint) *HiberaAPI {
    if addrs == "" {
        // If no addrs are provided, try using
        // the environment variable. If this
        // environment variable is not set, we
        // use the default host and port.
        addrs = os.Getenv("HIBERA_API")
    } else {
        os.Setenv("HIBERA_API", addrs)
    }
    if auth == "" {
        // Same for auth.
        auth = os.Getenv("HIBERA_AUTH")
    } else {
        os.Setenv("HIBERA_AUTH", auth)
    }
    urls := utils.GenerateURLs(addrs, DefaultHost, DefaultPort)
    clientid := generateClientId()
    return NewHiberaAPI(urls, auth, clientid, delay)
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
    body    []byte
}

func (h *HiberaAPI) makeArgs(path string) HttpArgs {
    headers := make(map[string]string)
    params := make(map[string]string)
    return HttpArgs{path, headers, params, nil}
}

func (h *HiberaAPI) getClusterId(resp *http.Response) string {
    ids := resp.Header["X-Cluster-Id"]
    if len(ids) == 0 {
        return ""
    }
    return ids[0]
}

func (h *HiberaAPI) getRev(resp *http.Response) (uint64, error) {
    rev := resp.Header["X-Revision"]
    if len(rev) == 0 {
        return 0, NoRevision
    }
    revint, err := strconv.ParseUint(rev[0], 0, 64)
    if err == nil {
        utils.Print("CLIENT", "REVISION %d", revint)
    }
    return revint, err
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
    host := h.urls[rand.Int()%len(h.urls)]
    url := utils.MakeURL(host, args.path, args.params)
    req, err := http.NewRequest(method, url, bytes.NewBuffer(args.body))
    if err != nil {
        return req, err
    }

    // Append headers.
    req.Header.Add("X-Client-Id", h.clientid)
    req.Header.Add("X-Authorization", h.auth)
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

        for err == nil {

            utils.Print("CLIENT", "%s %s", method, req.URL.String())
            resp, err = h.Client.Do(req)
            if err != nil {
                urlerr, ok := err.(*url.Error)
                if !ok || urlerr.Err != skipRedirect {
                    break
                }
            }
            utils.Print("CLIENT", "%s", resp.Status)

            // Check for a permission problem (don't retry).
            if resp.StatusCode == http.StatusForbidden {
                return resp, err
            }

            // Check for a redirect.
            if resp.StatusCode == http.StatusMovedPermanently ||
               resp.StatusCode == http.StatusFound ||
               resp.StatusCode == http.StatusSeeOther ||
               resp.StatusCode == http.StatusTemporaryRedirect {
                // Read the next location.
                location := resp.Header.Get("Location")
                utils.Print("CLIENT", "Redirecting to %s...", location)
                var u *url.URL
                u, err = url.Parse(location)
                if err != nil {
                    break
                }

                // Change the request.
                req, err = h.makeRequest(method, args)
                req.URL = u
                req.Host = u.Host
            } else {
                return resp, err
            }
        }
        if h.delay == 0 {
            return resp, err
        }

        // Print a message to the console and retry.
        random_delay := (rand.Int() % int(h.delay * 2)) + 1
        utils.Print("CLIENT", "ERROR %s (delaying %d milliseconds)", err, random_delay)
        time.Sleep(time.Duration(random_delay) * time.Millisecond)
    }
    return nil, nil
}

func (h *HiberaAPI) Info(rev uint64) (string, []byte, uint64, error) {
    args := h.makeArgs("/")
    args.params["rev"] = strconv.FormatUint(rev, 10)
    resp, err := h.doRequest("GET", args)
    if err != nil {
        return "", nil, 0, err
    }
    id := h.getClusterId(resp)
    content, err := h.getContent(resp)
    if err != nil {
        rev, _ = h.getRev(resp)
        return id, nil, rev, err
    }
    rev, err = h.getRev(resp)
    return id, content, rev, err
}

func (h* HiberaAPI) Activate() error {
    args := h.makeArgs("/")
    _, err := h.doRequest("POST", args)
    return err
}

func (h *HiberaAPI) Deactivate() error {
    args := h.makeArgs("/")
    _, err := h.doRequest("DELETE", args)
    return err
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
        rev, _ := h.getRev(resp)
        return -1, rev, errors.New(resp.Status)
    }
    content, err := h.getContent(resp)
    if err != nil {
        rev, _ := h.getRev(resp)
        return -1, rev, err
    }
    var index int
    err = json.Unmarshal(content, &index)
    if err != nil {
        rev, _ := h.getRev(resp)
        return -1, rev, err
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
        rev, _ := h.getRev(resp)
        return rev, errors.New(resp.Status)
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
        rev, _ := h.getRev(resp)
        return -1, nil, rev, errors.New(resp.Status)
    }
    content, err := h.getContent(resp)
    if err != nil {
        rev, _ := h.getRev(resp)
        return -1, nil, rev, err
    }
    var info syncInfo
    err = json.Unmarshal(content, &info)
    if err != nil {
        rev, _ := h.getRev(resp)
        return -1, nil, rev, err
    }
    rev, err := h.getRev(resp)
    return info.Index, info.Members, rev, err
}

func (h *HiberaAPI) Fire(key string, rev uint64) (uint64, error) {
    args := h.makeArgs(fmt.Sprintf("/event/%s", key))
    args.params["rev"] = strconv.FormatUint(rev, 10)
    resp, err := h.doRequest("POST", args)
    if err != nil {
        return 0, err
    }
    if resp.StatusCode != 200 {
        rev, _ = h.getRev(resp)
        return rev, errors.New(resp.Status)
    }
    rev, err = h.getRev(resp)
    return rev, err
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
        rev, _ = h.getRev(resp)
        return rev, errors.New(resp.Status)
    }
    rev, err = h.getRev(resp)
    return rev, err
}

func (h *HiberaAPI) Get(key string, rev uint64, timeout uint) ([]byte, uint64, error) {
    args := h.makeArgs(fmt.Sprintf("/data/%s", key))
    args.params["rev"] = strconv.FormatUint(rev, 10)
    args.params["timeout"] = strconv.FormatUint(uint64(timeout), 10)
    resp, err := h.doRequest("GET", args)
    if err != nil {
        return nil, 0, err
    }
    if resp.StatusCode != 200 {
        rev, _ := h.getRev(resp)
        return nil, rev, errors.New(resp.Status)
    }
    content, err := h.getContent(resp)
    if err != nil {
        return nil, 0, err
    }
    rev, err = h.getRev(resp)
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

func (h *HiberaAPI) Set(key string, rev uint64, value []byte) (uint64, error) {
    args := h.makeArgs(fmt.Sprintf("/data/%s", key))
    args.params["rev"] = strconv.FormatUint(rev, 10)
    args.body = value
    resp, err := h.doRequest("POST", args)
    if err != nil {
        return 0, err
    }
    if resp.StatusCode != 200 {
        rev, _ = h.getRev(resp)
        return rev, errors.New(resp.Status)
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
        rev, _ = h.getRev(resp)
        return rev, errors.New(resp.Status)
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

func (h *HiberaAPI) AuthGet(key string) ([]byte, error) {
    args := h.makeArgs(fmt.Sprintf("/auth/%s", key))
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
    return content, err
}

func (h *HiberaAPI) AuthList() ([]string, error) {
    args := h.makeArgs("/auth")
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

func (h *HiberaAPI) AuthUpdate(key string, value []byte) error {
    args := h.makeArgs(fmt.Sprintf("/auth/%s", key))
    args.body = value
    resp, err := h.doRequest("POST", args)
    if err != nil {
        return err
    }
    if resp.StatusCode != 200 {
        return errors.New(resp.Status)
    }
    return err
}

func (h *HiberaAPI) AuthRemove(key string) error {
    args := h.makeArgs(fmt.Sprintf("/auth/%s", key))
    resp, err := h.doRequest("DELETE", args)
    if err != nil {
        return err
    }
    if resp.StatusCode != 200 {
        return errors.New(resp.Status)
    }
    return err
}

func (h *HiberaAPI) AuthClear() error {
    args := h.makeArgs("/auth")
    resp, err := h.doRequest("DELETE", args)
    if err != nil {
        return err
    }
    if resp.StatusCode != 200 {
        return errors.New(resp.Status)
    }
    return err
}
