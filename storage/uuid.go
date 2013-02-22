package storage

import (
	"strings"
	"io/ioutil"
)

func Uuid() (string, error) {
	contents, err := ioutil.ReadFile("/proc/sys/kernel/random/uuid")
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(contents)), nil
}

func Uuids(n uint) ([]string, error) {
    uuids := make([]string, n, n)
    for i := uint(0); i < n; i += 1 {
        var err error
        uuids[i], err = Uuid()
        if err != nil {
            return nil, err
        }
    }
    return uuids, nil
}
