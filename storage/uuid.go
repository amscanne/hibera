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
