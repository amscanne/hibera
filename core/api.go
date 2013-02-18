package core

type API interface {
	Info() (Info, error)
	DataList() ([]string, error)
	DataClear() error
	LockOwners(key string) ([]string, uint64, error)
	LockAcquire(client *Client, key string, timeout uint64, name string, limit uint64) (uint64, error)
	LockRelease(client *Client, key string) (uint64, error)
	GroupMembers(group string, name string, limit uint64) ([]string, uint64, error)
	GroupLeave(client *Client, group string, name string) (uint64, error)
	DataGet(key string) ([]byte, uint64, error)
	DataSet(key string, value []byte, rev uint64) (uint64, error)
	DataRemove(key string, rev uint64) (uint64, error)
	WatchWait(client *Client, key string, rev uint64) (uint64, error)
	WatchFire(key string, rev uint64) (uint64, error)
}
