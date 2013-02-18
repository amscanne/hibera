package core

var DEFAULT_KEYS = uint(128)

type Cluster struct{}

func NewCluster(domain string, ids []string) *Cluster {
	return new(Cluster)
}
