package core

import (
	"log"
	"io/ioutil"
)

func Uuid() string {
	contents, err := ioutil.ReadFile("/proc/sys/kernel/random/uuid")

	if err != nil {
		log.Fatal("UUID generation failed!")
		return ""
	}

	return string(contents)
}
