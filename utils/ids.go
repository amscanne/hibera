package utils

import (
    "bytes"
    "io/ioutil"
    "strings"
)

func LoadIds(path string, number uint) ([]string, error) {
    ids := make([]string, 0)

    // Read our current set of ids.
    iddata, err := ioutil.ReadFile(path)
    if err != nil &&
        iddata != nil ||
        len(iddata) > 0 {
        ids = strings.Split(string(iddata), "\n")
    }

    // Supplement.
    for {
        if len(ids) >= int(number) {
            break
        }
        uuid, err := Uuid()
        if err != nil {
            return nil, err
        }
        ids = append(ids, uuid)
    }

    // Write out the result.
    buf := bytes.NewBuffer(make([]byte, 0))
    for _, id := range ids {
        buf.WriteString(id)
        buf.WriteString("\n")
    }
    err = ioutil.WriteFile(path, buf.Bytes(), 0644)
    if err != nil {
        return nil, err
    }

    // Return our ids.
    return ids[0:number], nil
}
