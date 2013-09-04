package core

import (
    "errors"
    "math/big"
)

var NoRevision = Revision{big.NewInt(0)}
var DecodeError = errors.New("decode error")

func RevisionFromBytes(metadata []byte) Revision {
    rev := big.NewInt(0)
    rev.SetBytes(metadata)
    return Revision{rev}
}

func RevisionFromString(rev_str string) (Revision, error) {
    rev := big.NewInt(0)
    rev, ok := rev.SetString(rev_str, 0)
    if !ok {
        return NoRevision, DecodeError
    }
    return Revision{rev}, nil
}

func (r Revision) Bytes() []byte {
    return (r.Int).Bytes()
}

func (r Revision) Next() Revision {
    ref := r.Int
    if ref == nil {
        ref = NoRevision.Int
    }
    inc_rev := big.NewInt(1)
    return Revision{inc_rev.Add(inc_rev, ref)}
}

func (r Revision) IsZero() bool {
    ref := r.Int
    if ref == nil {
        ref = NoRevision.Int
    }
    return (ref).Cmp(NoRevision.Int) == 0
}

func (r Revision) Equals(other Revision) bool {
    ref := r.Int
    if ref == nil {
        ref = NoRevision.Int
    }
    other_ref := other.Int
    if other_ref == nil {
        other_ref = NoRevision.Int
    }
    return (ref).Cmp(other_ref) == 0
}

func (r Revision) GreaterThan(other Revision) bool {
    ref := r.Int
    if ref == nil {
        ref = NoRevision.Int
    }
    other_ref := other.Int
    if other_ref == nil {
        other_ref = NoRevision.Int
    }
    return (ref).Cmp(other_ref) > 0
}

func (r Revision) String() string {
    ref := r.Int
    if ref == nil {
        ref = NoRevision.Int
    }
    return (ref).String()
}

func (r *Revision) UnmarshalJSON(data []byte) error {
    other, err := RevisionFromString(string(data))
    if err != nil {
        return err
    }
    r.Int = other.Int
    return nil
}

func (r *Revision) MarshalJSON() ([]byte, error) {
    return []byte(r.String()), nil
}
