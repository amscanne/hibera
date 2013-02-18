package storage

import (
	"log"
	"strings"
	"io/ioutil"
)

func Uuid() (string, error) {
	contents, err := ioutil.ReadFile("/proc/sys/kernel/random/uuid")
	if err != nil {
		log.Fatal("UUID generation failed!")
		return "", err
	}

	return strings.TrimSpace(string(contents)), nil
}
