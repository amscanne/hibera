package core

type Key string
type Revision uint64
type EphemId uint64
type SubName string

type API interface {
	DataList() (*[]Key, error)
	DataClear() error

	LockOwners(key Key, name SubName) ([]SubName, Revision, error)
	LockAcquire(id EphemId, key Key, timeout uint64, name SubName, limit uint64) (Revision, error)
	LockRelease(id EphemId, key Key, name SubName) (Revision, error)

        GroupJoin(id EphemId, group Key, name SubName) (Revision, error)
	GroupMembers(group Key, name SubName, limit uint64) ([]SubName, Revision, error)
	GroupLeave(id EphemId, group Key, name SubName) (Revision, error)

	DataGet(key Key) ([]byte, Revision, error)
	DataSet(key Key, value []byte, rev Revision) (Revision, error)
	DataRemove(key Key, rev Revision) (Revision, error)

	WatchWait(id EphemId, key Key, rev Revision, timeout uint64) (Revision, error)
	WatchFire(key Key, rev Revision) (Revision, error)
}
