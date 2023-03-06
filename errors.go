package redlock

import "fmt"

type LockError struct {
	resource string
}

func (l LockError) Error() string {
	return fmt.Sprintf("failed to acquire lock on '%s'", l.resource)
}

func IsLockError(err error) bool {
	_, ok := err.(LockError)
	return ok
}

type StaleLockError struct{}

func (StaleLockError) Error() string {
	return "local lock information is stale and the lock is already being held by another client"
}

func IsStaleError(err error) bool {
	_, ok := err.(StaleLockError)
	return ok
}
