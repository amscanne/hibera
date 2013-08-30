package client

import (
    "bytes"
    "crypto/tls"
    "encoding/json"
    "errors"
    "fmt"
    "hibera/core"
    "hibera/utils"
    "io"
    "math/rand"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "time"
)

var NoRevision = errors.New("No X-Revision Found")

type HiberaAPI struct {
    urls         []string
    auth         string
    clientid     string
    delay        uint
    useRedirects bool
    cache        map[string]string
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

func boolToStr(val bool) string {
    if val {
        return "true"
    }
    return "false"
}

func NewHiberaAPI(
    urls []string,
    auth string,
    clientid string,
    delay uint,
    useRedirects bool) *HiberaAPI {

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
    api.useRedirects = useRedirects
    api.cache = make(map[string]string)

    for _, url := range urls {
        utils.Print("CLIENT", "API %s", url)
    }

    // Create our HTTP transport.
    // Nothing really special about this, but we may want
    // to tune parameters for idling connections, etc.
    tr := &http.Transport{
        TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
        DisableKeepAlives: true,
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
    urls := utils.GenerateURLs(addrs, utils.DefaultHost, utils.DefaultPort)
    clientid := generateClientId()
    return NewHiberaAPI(urls, auth, clientid, delay, true)
}

type httpArgs struct {
    namespace core.Namespace
    path      string
    headers   map[string]string
    params    map[string]string
    body      []byte
}

func (h *HiberaAPI) makeArgs(namespace core.Namespace, path string) httpArgs {
    headers := make(map[string]string)
    params := make(map[string]string)
    return httpArgs{namespace, path, headers, params, nil}
}

func (h *HiberaAPI) getRev(resp *http.Response) (core.Revision, error) {
    rev := resp.Header["X-Revision"]
    if len(rev) == 0 {
        return core.ZeroRevision, NoRevision
    }
    intrev, ok := core.ParseRevision(rev[0])
    if ok {
        utils.Print("CLIENT", "REVISION %s", (*intrev).String())
        return intrev, nil
    }
    return intrev, NoRevision
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

func (h *HiberaAPI) makeRequest(method string, args httpArgs, hint string) (*http.Request, error) {
    url, ok := h.cache[hint]
    if ok {
        // Select the cached URL.
        url = utils.MakeURL(url, args.path, args.params)
    } else {
        // Select a random host to make a request.
        host := h.urls[rand.Int()%len(h.urls)]
        url = utils.MakeURL(host, args.path, args.params)
    }
    req, err := http.NewRequest(method, url, bytes.NewBuffer(args.body))
    if err != nil {
        return req, err
    }

    // Append headers.
    req.Header.Add("X-Client-Id", h.clientid)
    req.Header.Add("X-Authorization", h.auth)
    if args.namespace != "" {
        req.Header.Add("Host", string(args.namespace))
    }
    for key, value := range args.headers {
        req.Header.Add(key, value)
    }

    return req, nil
}

func (h *HiberaAPI) doRequest(method string, args httpArgs, hint string) ([]byte, core.Revision, error) {
    for {
        // Try the actual request.
        req, err := h.makeRequest(method, args, hint)

        for {
            var resp *http.Response
            var content []byte

            utils.Print("CLIENT", "%s %s", method, req.URL.String())
            resp, err = h.Client.Do(req)
            if resp != nil {
                utils.Print("CLIENT", "%s", resp.Status)
            }

            if err != nil {
                urlerr, ok := err.(*url.Error)
                if resp != nil && ok && urlerr.Err == skipRedirect {
                    if !h.useRedirects {
                        return nil, core.ZeroRevision, err
                    } else {
                        // All is okay -- we process the redirect below.
                        // NOTE: In this case, the body has already been
                        // read and closed so we don't close the body in
                        // the case of a redirect as per below.
                    }
                } else {
                    // Not okay, delay.
                    break
                }
            } else {
                content, err = h.getContent(resp)
                resp.Body.Close()
                if err != nil {
                    rev, _ := h.getRev(resp)
                    return nil, rev, err
                }
            }

            // Check for a permission problem (don't retry).
            if resp.StatusCode == http.StatusForbidden {
                err = os.ErrPermission
                break
            }
            if resp.StatusCode == http.StatusServiceUnavailable {
                err = os.ErrInvalid
                break
            }

            if resp.StatusCode == http.StatusOK {

                // Everything okay.
                rev, err := h.getRev(resp)
                return content, rev, err

            } else if resp.StatusCode == http.StatusMovedPermanently ||
                resp.StatusCode == http.StatusFound ||
                resp.StatusCode == http.StatusSeeOther ||
                resp.StatusCode == http.StatusTemporaryRedirect {

                // Handle a redirect.
                location := resp.Header.Get("Location")
                utils.Print("CLIENT", "Redirecting to %s...", location)
                var u *url.URL
                u, err = url.Parse(location)
                if err != nil {
                    break
                }

                // Change the request.
                req, err = h.makeRequest(method, args, hint)
                req.URL = u
                req.Host = u.Host

                // Save the new cached value.
                h.cache[hint] = u.Host

            } else {

                // Return the given HTTP error.
                rev, _ := h.getRev(resp)
                return nil, rev, errors.New(resp.Status)
            }
        }

        if h.delay == 0 {
            return nil, core.ZeroRevision, err
        }

        // Clear any cache hint on an error.
        delete(h.cache, hint)

        // Print a message to the console and retry.
        random_delay := (rand.Int() % int(h.delay*2)) + 1
        utils.Print("CLIENT", "ERROR %s (delaying %d milliseconds)", err, random_delay)
        time.Sleep(time.Duration(random_delay) * time.Millisecond)
    }

    return nil, core.ZeroRevision, nil
}

func (h *HiberaAPI) Info() ([]byte, core.Revision, error) {
    args := h.makeArgs("", "/v1.0/")
    return h.doRequest("GET", args, "")
}

func (h *HiberaAPI) Activate(replication uint) error {
    args := h.makeArgs("", "/v1.0/")
    args.params["replication"] = string(replication)
    _, _, err := h.doRequest("POST", args, "")
    return err
}

func (h *HiberaAPI) Deactivate() error {
    args := h.makeArgs("", "/v1.0/")
    _, _, err := h.doRequest("DELETE", args, "")
    return err
}

func (h *HiberaAPI) NodeList(active bool) ([]string, core.Revision, error) {
    args := h.makeArgs("", "/v1.0/nodes")
    args.params["active"] = boolToStr(active)
    content, rev, err := h.doRequest("GET", args, "")
    if err != nil {
        return nil, rev, err
    }
    var nodes []string
    err = json.Unmarshal(content, &nodes)
    if err != nil {
        return nil, rev, err
    }
    return nodes, rev, err
}

func (h *HiberaAPI) NodeGet(id string) (*core.Node, core.Revision, error) {
    args := h.makeArgs("", fmt.Sprintf("/v1.0/nodes/%s", id))
    content, rev, err := h.doRequest("GET", args, id)
    if err != nil {
        return nil, core.ZeroRevision, err
    }
    var node core.Node
    err = json.Unmarshal(content, &node)
    if err != nil {
        return nil, rev, err
    }
    return &node, rev, err
}

func (h *HiberaAPI) AccessList(namespace core.Namespace) ([]string, core.Revision, error) {
    args := h.makeArgs(namespace, "/v1.0/access")
    content, rev, err := h.doRequest("GET", args, "")
    if err != nil {
        return nil, core.ZeroRevision, err
    }
    var tokens []string
    err = json.Unmarshal(content, &tokens)
    if err != nil {
        return nil, rev, err
    }
    return tokens, rev, err
}

func (h *HiberaAPI) AccessUpdate(auth core.Key, path string, read bool, write bool, execute bool) (core.Revision, error) {
    args := h.makeArgs(auth.Namespace, fmt.Sprintf("/v1.0/access/%s", auth.Key))
    args.params["path"] = path
    args.params["read"] = boolToStr(read)
    args.params["write"] = boolToStr(write)
    args.params["execute"] = boolToStr(execute)
    _, rev, err := h.doRequest("POST", args, "")
    if err != nil {
        return core.ZeroRevision, err
    }
    return rev, err
}

func (h *HiberaAPI) AccessGet(auth core.Key) (*core.Token, core.Revision, error) {
    args := h.makeArgs(auth.Namespace, fmt.Sprintf("/v1.0/access/%s", auth.Key))
    content, rev, err := h.doRequest("GET", args, "")
    if err != nil {
        return nil, core.ZeroRevision, err
    }
    var val core.Token
    err = json.Unmarshal(content, &val)
    if err != nil {
        return nil, rev, err
    }
    return &val, rev, err
}

func (h *HiberaAPI) SyncJoin(key core.Key, name string, limit uint, timeout uint) (int, core.Revision, error) {
    args := h.makeArgs(key.Namespace, fmt.Sprintf("/v1.0/sync/%s", key))
    args.params["name"] = name
    args.params["limit"] = strconv.FormatUint(uint64(limit), 10)
    args.params["timeout"] = strconv.FormatUint(uint64(timeout), 10)
    content, rev, err := h.doRequest("POST", args, key.Key)
    if err != nil {
        return -1, core.ZeroRevision, err
    }
    var index int
    err = json.Unmarshal(content, &index)
    if err != nil {
        return -1, rev, err
    }
    return index, rev, err
}

func (h *HiberaAPI) SyncLeave(key core.Key, name string) (core.Revision, error) {
    args := h.makeArgs(key.Namespace, fmt.Sprintf("/v1.0/sync/%s", key))
    args.params["name"] = name
    _, rev, err := h.doRequest("DELETE", args, key.Key)
    return rev, err
}

func (h *HiberaAPI) SyncMembers(key core.Key, name string, limit uint) (int, []string, core.Revision, error) {
    args := h.makeArgs(key.Namespace, fmt.Sprintf("/v1.0/sync/%s", key))
    args.params["name"] = name
    args.params["limit"] = strconv.FormatUint(uint64(limit), 10)
    content, rev, err := h.doRequest("GET", args, key.Key)
    if err != nil {
        return -1, nil, core.ZeroRevision, err
    }
    var info core.SyncInfo
    err = json.Unmarshal(content, &info)
    if err != nil {
        return -1, nil, rev, err
    }
    return info.Index, info.Members, rev, err
}

func (h *HiberaAPI) EventFire(key core.Key, rev core.Revision) (core.Revision, error) {
    args := h.makeArgs(key.Namespace, fmt.Sprintf("/v1.0/event/%s", key))
    args.params["rev"] = (*rev).String()
    _, rev, err := h.doRequest("POST", args, key.Key)
    return rev, err
}

func (h *HiberaAPI) EventWait(key core.Key, rev core.Revision, timeout uint) (core.Revision, error) {
    args := h.makeArgs(key.Namespace, fmt.Sprintf("/v1.0/event/%s", key))
    args.params["rev"] = (*rev).String()
    args.params["timeout"] = strconv.FormatUint(uint64(timeout), 10)
    _, rev, err := h.doRequest("GET", args, key.Key)
    return rev, err
}

func (h *HiberaAPI) DataGet(key core.Key, rev core.Revision, timeout uint) ([]byte, core.Revision, error) {
    args := h.makeArgs(key.Namespace, fmt.Sprintf("/v1.0/data/%s", key))
    args.params["rev"] = (*rev).String()
    args.params["timeout"] = strconv.FormatUint(uint64(timeout), 10)
    return h.doRequest("GET", args, key.Key)
}

func (h *HiberaAPI) DataList(namespace core.Namespace) ([]string, error) {
    args := h.makeArgs(namespace, "/v1.0/data")
    content, _, err := h.doRequest("GET", args, "")
    var items []string
    err = json.Unmarshal(content, &items)
    if err != nil {
        return nil, err
    }
    return items, nil
}

func (h *HiberaAPI) DataSet(key core.Key, rev core.Revision, value []byte) (core.Revision, error) {
    args := h.makeArgs(key.Namespace, fmt.Sprintf("/v1.0/data/%s", key))
    args.params["rev"] = (*rev).String()
    args.body = value
    _, rev, err := h.doRequest("POST", args, key.Key)
    return rev, err
}

func (h *HiberaAPI) DataRemove(key core.Key, rev core.Revision) (core.Revision, error) {
    args := h.makeArgs(key.Namespace, fmt.Sprintf("/v1.0/data/%s", key))
    args.params["rev"] = (*rev).String()
    _, rev, err := h.doRequest("DELETE", args, key.Key)
    return rev, err
}
