package client

type HiberaClient struct {
}

func NewHiberaClient(url string) *HiberaClient {
    return new(HiberaClient)
}

func (h *HiberaClient) Lock(key string, timeout uint, name string) error {
    return nil
}

func (h *HiberaClient) Release(key string) error {
    return nil
}

func (h *HiberaClient) State(key string) (string, uint, error) {
    return "", 0, nil
}

func (h *HiberaClient) Watch(key string, rev uint) (uint, error) {
    return 0, nil
}

func (h *HiberaClient) Join(group string, name string) error {
    return nil
}

func (h *HiberaClient) Leave(group string, name string) error {
    return nil
}

func (h *HiberaClient) Members(group string, limit uint) ([]string, uint, error) {
    return nil, 0, nil
}

func (h *HiberaClient) Get(key string) (string, uint, error) {
    return "", 0, nil
}

func (h *HiberaClient) Set(key string, value string, rev uint) (uint, error) {
    return 0, nil
}

func (h *HiberaClient) Clear(key string, rev uint) error {
    return nil
}

func (h *HiberaClient) Fire(key string, rev uint) error {
    return nil
}
