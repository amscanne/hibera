package storage

import (
    "os"
)

func nop_run(*os.File, *int64) ([]byte, error) {
    return nil, nil
}

func nop_cancel() {
}

func nop_io() error {
    return nil
}

func nop_read(*os.File, *int64) error {
    return nil
}
