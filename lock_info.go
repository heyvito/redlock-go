package redlock

import "time"

type LockInfo struct {
	parent    *redlock
	Validity  int
	ExpiresAt time.Time
	resource  string
	value     string
}

func (l *LockInfo) Unlock() {
	l.parent.Unlock(l)
}

func (l *LockInfo) IsValid() (bool, error) {
	return l.parent.IsLockValid(l)
}
