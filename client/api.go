package client

import (
    "bytes"
    "crypto/tls"
    "encoding/json"
    "errors"
    "fmt"
    "github.com/amscanne/hibera/core"
    "github.com/amscanne/hibera/utils"
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
    auth         core.Token
    clientid     string
    delay        uint
    defaultNS    core.Namespace
    useRedirects bool
    cache        map[string]string
    *http.Client
    *http.Transport
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

func NewHiberaAPI(urls []string, auth core.Token, clientid string, delay uint, defaultNS core.Namespace, useRedirects bool) *HiberaAPI {

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
    api.defaultNS = defaultNS
    api.useRedirects = useRedirects
    api.cache = make(map[string]string)

    for _, url := range urls {
        utils.Print("CLIENT", "API %s", url)
    }

    // Create our HTTP transport.
    // Nothing really special about this, but we may want
    // to tune parameters for idling connections, etc.
    api.Transport = &http.Transport{
        TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
        MaxIdleConnsPerHost: 1,
    }
    api.Client = &http.Client{Transport: api.Transport, CheckRedirect: noRedirect}

    return api
}

func NewHiberaClient(addrs string, auth string, delay uint, namespace string) *HiberaAPI {
    if addrs == "" {
        // If no addrs are provided, try using
        // the environment variable. If this
        // environment variable is not set, we
        // use the default host and port.
        addrs = os.Getenv("HIBERA_API")
    } else {
        os.Setenv("HIBERA_API", addrs)
    }
    if addrs == "" {
        // Still nothing?
        // Use the default.
        addrs = fmt.Sprintf("%s:%d", utils.DefaultHost, utils.DefaultPort)
    }
    if auth == "" {
        // Same for auth.
        auth = os.Getenv("HIBERA_AUTH")
    } else {
        os.Setenv("HIBERA_AUTH", auth)
    }
    if namespace == "" {
        // Same for namespace.
        namespace = os.Getenv("HIBERA_NAMESPACE")
    } else {
        os.Setenv("HIBERA_NAMESPACE", namespace)
    }
    urls := utils.URLs(addrs)
    clientid := generateClientId()
    return NewHiberaAPI(urls, core.Token(auth), clientid, delay, core.Namespace(namespace), true)
}

func (h *HiberaAPI) Close() {
    h.Transport.CloseIdleConnections()
}

type httpArgs struct {
    ns      core.Namespace
    path    string
    headers map[string]string
    params  map[string]string
    body    []byte
}

func (h *HiberaAPI) makeArgs(ns core.Namespace, path string) httpArgs {
    headers := make(map[string]string)
    params := make(map[string]string)
    return httpArgs{ns, path, headers, params, nil}
}

func (h *HiberaAPI) getRev(resp *http.Response) (core.Revision, error) {
    rev_header := resp.Header["X-Revision"]
    if len(rev_header) == 0 {
        return core.NoRevision, NoRevision
    }
    rev, err := core.RevisionFromString(rev_header[0])
    if err != nil {
        return core.NoRevision, err
    }
    utils.Print("CLIENT", "REVISION %s", rev.String())
    return rev, nil
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
        url = h.urls[rand.Int()%len(h.urls)]
        url = utils.MakeURL(url, args.path, args.params)
    }
    req, err := http.NewRequest(method, url, bytes.NewBuffer(args.body))
    if err != nil {
        return req, err
    }

    // Append headers.
    req.Header.Add("X-Client-Id", h.clientid)
    req.Header.Add("X-Authorization", string(h.auth))
    req.Header.Add("X-Namespace", string(args.ns))
    for key, value := range args.headers {
        req.Header.Add(key, value)
    }

    return req, nil
}

func (h *HiberaAPI) Delay() bool {
    if h.delay > 0 {
        random_delay := (rand.Int() % int(h.delay*2)) + 1
        utils.Print("CLIENT", "DELAY %d milliseconds", random_delay)
        time.Sleep(time.Duration(random_delay) * time.Millisecond)
        return true
    }
    return false
}

func (h *HiberaAPI) doRequest(method string, args httpArgs, hint string, retry bool) ([]byte, core.Revision, error) {
    for {
        // Try the actual request.
        req, err := h.makeRequest(method, args, hint)

        for {
            var resp *http.Response
            var content []byte

            utils.Print("CLIENT", "%s %s", method, req.URL.String())
            if args.body != nil {
                utils.Print("CLIENT", "BODY %s", string(args.body))
            }
            resp, err = h.Client.Do(req)
            if resp != nil {
                utils.Print("CLIENT", "%s", resp.Status)
            }

            if err != nil {
                urlerr, ok := err.(*url.Error)
                if resp != nil && ok && urlerr.Err == skipRedirect {
                    if !h.useRedirects {
                        return nil, core.NoRevision, err
                    } else {
                        // All is okay -- we process the redirect below.
                        // NOTE: In this case, the body has already been
                        // read and closed so we don't close the body in
                        // the case of a redirect as per below.
                    }
                } else {
                    // Clear any cache hint on an error.
                    utils.Print("CLIENT", "ERROR %s", err)
                    delete(h.cache, hint)

                    // If retry is on, then we will retry the request
                    // below. If it is not, then we will return the error.
                    // This is used for some synchronization requests
                    // (such as SyncMembers and SyncLeave) that depend
                    // on using the same underlying connection.
                    if !retry {
                        return nil, core.NoRevision, err
                    } else {
                        break
                    }
                }
            } else {
                content, err = h.getContent(resp)
                resp.Body.Close()
                if err != nil {
                    rev, _ := h.getRev(resp)
                    return nil, rev, err
                }
            }

            // Check for a permission problem..
            if resp.StatusCode == http.StatusForbidden {
                return nil, core.NoRevision, os.ErrPermission
            }

            // Check for a revision conflict.
            if resp.StatusCode == http.StatusConflict {
                rev, _ := h.getRev(resp)
                return nil, rev, core.RevConflict
            }

            if resp.StatusCode == http.StatusOK {

                // Everything okay.
                rev, err := h.getRev(resp)
                return content, rev, err

            } else if resp.StatusCode == http.StatusInternalServerError {

                // Temporary internal failure.
                // Usually this is a quorum failure,
                // which results from mulitple calls
                // with set rev=0.
                break

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

        if !h.Delay() {
            break
        }
    }

    return nil, core.NoRevision, nil
}

func (h *HiberaAPI) Info(retry bool) (*core.Info, core.Revision, error) {
    args := h.makeArgs("", "/v1.0/")
    content, rev, err := h.doRequest("GET", args, "", retry)
    if err != nil {
        return nil, core.NoRevision, err
    }
    info := core.NewInfo()
    err = json.Unmarshal(content, info)
    if err != nil {
        return nil, rev, err
    }
    return info, rev, err
}

func (h *HiberaAPI) Activate(replication uint) error {
    args := h.makeArgs("", "/v1.0/")
    args.params["replication"] = strconv.FormatUint(uint64(replication), 10)
    _, _, err := h.doRequest("POST", args, "", true)
    return err
}

func (h *HiberaAPI) Deactivate() error {
    args := h.makeArgs("", "/v1.0/")
    _, _, err := h.doRequest("DELETE", args, "", false)
    return err
}

func (h *HiberaAPI) AccessList() ([]string, core.Revision, error) {
    return h.NSAccessList(h.defaultNS)
}

func (h *HiberaAPI) NSAccessList(ns core.Namespace) ([]string, core.Revision, error) {
    args := h.makeArgs(ns, "/v1.0/access")
    content, rev, err := h.doRequest("GET", args, "", true)
    if err != nil {
        return nil, core.NoRevision, err
    }
    var tokens []string
    err = json.Unmarshal(content, &tokens)
    if err != nil {
        return nil, rev, err
    }
    return tokens, rev, err
}

func (h *HiberaAPI) AccessUpdate(auth string, path string, read bool, write bool, execute bool) (core.Revision, error) {
    return h.NSAccessUpdate(h.defaultNS, core.Token(auth), path, read, write, execute)
}

func (h *HiberaAPI) NSAccessUpdate(ns core.Namespace, auth core.Token, path string, read bool, write bool, execute bool) (core.Revision, error) {
    args := h.makeArgs(ns, fmt.Sprintf("/v1.0/access/%s", string(auth)))
    args.params["path"] = path
    args.params["read"] = fmt.Sprintf("%t", read)
    args.params["write"] = fmt.Sprintf("%t", write)
    args.params["execute"] = fmt.Sprintf("%t", execute)
    _, rev, err := h.doRequest("POST", args, "", true)
    if err != nil {
        return core.NoRevision, err
    }
    return rev, err
}

func (h *HiberaAPI) AccessGet(auth string) (*core.Permissions, core.Revision, error) {
    return h.NSAccessGet(h.defaultNS, core.Token(auth))
}

func (h *HiberaAPI) NSAccessGet(ns core.Namespace, auth core.Token) (*core.Permissions, core.Revision, error) {
    args := h.makeArgs(ns, fmt.Sprintf("/v1.0/access/%s", string(auth)))
    content, rev, err := h.doRequest("GET", args, "", true)
    if err != nil {
        return nil, core.NoRevision, err
    }
    var val core.Permissions
    err = json.Unmarshal(content, &val)
    if err != nil {
        return nil, rev, err
    }
    return &val, rev, err
}

func (h *HiberaAPI) SyncJoin(key string, data string, limit uint, timeout uint) (int, core.Revision, error) {
    return h.NSSyncJoin(h.defaultNS, core.Key(key), data, limit, timeout)
}

func (h *HiberaAPI) NSSyncJoin(ns core.Namespace, key core.Key, data string, limit uint, timeout uint) (int, core.Revision, error) {
    args := h.makeArgs(ns, fmt.Sprintf("/v1.0/sync/%s", string(key)))
    args.params["data"] = data
    args.params["limit"] = strconv.FormatUint(uint64(limit), 10)
    args.params["timeout"] = strconv.FormatUint(uint64(timeout), 10)
    content, rev, err := h.doRequest("POST", args, string(key), false)
    if err != nil {
        return -1, core.NoRevision, err
    }
    var index int
    err = json.Unmarshal(content, &index)
    if err != nil {
        return -1, rev, err
    }
    return index, rev, err
}

func (h *HiberaAPI) SyncLeave(key string, data string) (bool, core.Revision, error) {
    return h.NSSyncLeave(h.defaultNS, core.Key(key), data)
}

func (h *HiberaAPI) NSSyncLeave(ns core.Namespace, key core.Key, data string) (bool, core.Revision, error) {
    args := h.makeArgs(ns, fmt.Sprintf("/v1.0/sync/%s", string(key)))
    args.params["data"] = data
    _, rev, err := h.doRequest("DELETE", args, string(key), false)
    if err != nil && err != core.RevConflict {
        return false, rev, err
    }
    if err == core.RevConflict {
        return false, rev, nil
    }
    return true, rev, nil
}

func (h *HiberaAPI) SyncMembers(key string, data string, limit uint) (int, []string, core.Revision, error) {
    return h.NSSyncMembers(h.defaultNS, core.Key(key), data, limit)
}

func (h *HiberaAPI) NSSyncMembers(ns core.Namespace, key core.Key, data string, limit uint) (int, []string, core.Revision, error) {
    args := h.makeArgs(ns, fmt.Sprintf("/v1.0/sync/%s", string(key)))
    args.params["data"] = data
    args.params["limit"] = strconv.FormatUint(uint64(limit), 10)
    content, rev, err := h.doRequest("GET", args, string(key), false)
    if err != nil {
        return -1, nil, core.NoRevision, err
    }
    var info core.SyncInfo
    err = json.Unmarshal(content, &info)
    if err != nil {
        return -1, nil, rev, err
    }
    return info.Index, info.Members, rev, err
}

func (h *HiberaAPI) EventFire(key string, rev core.Revision) (bool, core.Revision, error) {
    return h.NSEventFire(h.defaultNS, core.Key(key), rev)
}

func (h *HiberaAPI) NSEventFire(ns core.Namespace, key core.Key, rev core.Revision) (bool, core.Revision, error) {
    args := h.makeArgs(ns, fmt.Sprintf("/v1.0/event/%s", string(key)))
    args.params["rev"] = rev.String()
    _, rev, err := h.doRequest("POST", args, string(key), false)
    if err != nil && err != core.RevConflict {
        return false, rev, err
    }
    if err == core.RevConflict {
        return false, rev, nil
    }
    return true, rev, nil
}

func (h *HiberaAPI) EventWait(key string, rev core.Revision, timeout uint) (core.Revision, error) {
    return h.NSEventWait(h.defaultNS, core.Key(key), rev, timeout)
}

func (h *HiberaAPI) NSEventWait(ns core.Namespace, key core.Key, rev core.Revision, timeout uint) (core.Revision, error) {
    args := h.makeArgs(ns, fmt.Sprintf("/v1.0/event/%s", string(key)))
    args.params["rev"] = rev.String()
    args.params["timeout"] = strconv.FormatUint(uint64(timeout), 10)
    _, rev, err := h.doRequest("GET", args, string(key), false)
    return rev, err
}

func (h *HiberaAPI) DataInfo(key string, rev core.Revision, timeout uint) (core.Revision, error) {
    return h.NSDataInfo(h.defaultNS, core.Key(key), rev, timeout, true)
}

func (h *HiberaAPI) NSDataInfo(ns core.Namespace, key core.Key, rev core.Revision, timeout uint, retry bool) (core.Revision, error) {
    args := h.makeArgs(ns, fmt.Sprintf("/v1.0/data/%s", string(key)))
    args.params["rev"] = rev.String()
    args.params["timeout"] = strconv.FormatUint(uint64(timeout), 10)
    _, rev, err := h.doRequest("HEAD", args, string(key), retry)
    return rev, err
}

func (h *HiberaAPI) DataGet(key string, rev core.Revision, timeout uint) ([]byte, core.Revision, error) {
    return h.NSDataGet(h.defaultNS, core.Key(key), rev, timeout, true)
}

func (h *HiberaAPI) NSDataGet(ns core.Namespace, key core.Key, rev core.Revision, timeout uint, retry bool) ([]byte, core.Revision, error) {
    args := h.makeArgs(ns, fmt.Sprintf("/v1.0/data/%s", string(key)))
    args.params["rev"] = rev.String()
    args.params["timeout"] = strconv.FormatUint(uint64(timeout), 10)
    return h.doRequest("GET", args, string(key), retry)
}

func (h *HiberaAPI) DataList() (map[string]uint, error) {
    items, err := h.NSDataList(h.defaultNS, true)
    if items == nil || err != nil {
        return nil, err
    }
    keys := make(map[string]uint)
    for item, count := range items {
        keys[string(item)] = count
    }
    return keys, nil
}

func (h *HiberaAPI) NSDataList(ns core.Namespace, retry bool) (map[core.Key]uint, error) {
    args := h.makeArgs(ns, "/v1.0/data")
    content, _, err := h.doRequest("GET", args, "", retry)
    var items map[core.Key]uint
    if err != nil {
        return items, err
    }
    err = json.Unmarshal(content, &items)
    if err != nil {
        return nil, err
    }
    return items, nil
}

func (h *HiberaAPI) DataSet(key string, rev core.Revision, value []byte) (bool, core.Revision, error) {
    return h.NSDataSet(h.defaultNS, core.Key(key), rev, value)
}

func (h *HiberaAPI) NSDataSet(ns core.Namespace, key core.Key, rev core.Revision, value []byte) (bool, core.Revision, error) {
    args := h.makeArgs(ns, fmt.Sprintf("/v1.0/data/%s", string(key)))
    args.params["rev"] = rev.String()
    args.body = value
    _, out_rev, err := h.doRequest("POST", args, string(key), false)
    if err != nil && err != core.RevConflict {
        return false, out_rev, err
    }
    if err == core.RevConflict {
        return false, out_rev, nil
    } else if rev.IsZero() || rev.Equals(out_rev) {
        return true, out_rev, nil
    } else {
        return false, out_rev, nil
    }
}

func (h *HiberaAPI) DataRemove(key string, rev core.Revision) (bool, core.Revision, error) {
    return h.NSDataRemove(h.defaultNS, core.Key(key), rev)
}

func (h *HiberaAPI) NSDataRemove(ns core.Namespace, key core.Key, rev core.Revision) (bool, core.Revision, error) {
    args := h.makeArgs(ns, fmt.Sprintf("/v1.0/data/%s", string(key)))
    args.params["rev"] = rev.String()
    _, out_rev, err := h.doRequest("DELETE", args, string(key), false)
    if err != nil && err != core.RevConflict {
        return false, out_rev, err
    }
    if err == core.RevConflict {
        return false, out_rev, nil
    } else if rev.IsZero() || rev.Equals(out_rev) {
        return true, out_rev, err
    } else {
        return false, out_rev, err
    }
}
