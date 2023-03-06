package redlock

import (
	"github.com/redis/go-redis/v9"
	"math/rand"
	"sort"
	"time"

	"github.com/google/uuid"
)

type Redlock interface {
	Lock(resource string, ttl int) (*LockInfo, error)
	Locking(resource string, ttl int, fn func() error) error
	Unlock(info *LockInfo)
	RemainingTTLForLock(info *LockInfo) (int, error)
	RemainingTTLForResource(resource string) (int, error)
	IsLocked(resource string) (bool, error)
	IsLockValid(info *LockInfo) (bool, error)
	ExtendLock(info *LockInfo, newTTL int) error
	TryGetRemainingTTL(resource string) (ok bool, value string, ttl int, err error)
}

func New(clients ...*redis.Client) Redlock {
	instances := make([]*redisWrapper, len(clients))
	for i, c := range clients {
		instances[i] = &redisWrapper{c}
	}

	return &redlock{
		clients:          instances,
		quorum:           (len(clients) / 2) + 1,
		RetryCount:       3,
		RetryDelay:       200 * time.Millisecond,
		RetryJitter:      50,
		ClockDriftFactor: 0.01,
	}
}

type redlock struct {
	clients          []*redisWrapper
	RetryCount       int
	RetryDelay       time.Duration
	RetryDelayFn     func(attempt int) time.Duration
	RetryJitter      int
	ClockDriftFactor float64
	quorum           int
}

func (r *redlock) Lock(resource string, ttl int) (*LockInfo, error) {
	value := uuid.NewString()
	lockInfo := r.tryLockInstances(resource, value, ttl)
	if lockInfo == nil {
		return nil, LockError{resource: resource}
	}

	return lockInfo, nil
}

func (r *redlock) Locking(resource string, ttl int, fn func() error) error {
	li, err := r.Lock(resource, ttl)
	if err != nil {
		return err
	}
	defer r.Unlock(li)
	return fn()
}

func (r *redlock) Unlock(info *LockInfo) {
	r.unlock(info.resource, info.value)
}

func (r *redlock) unlock(resource, value string) {
	for _, c := range r.clients {
		c.unlock(resource, value)
	}
}

func (r *redlock) RemainingTTLForLock(info *LockInfo) (int, error) {
	ok, value, ttl, err := r.TryGetRemainingTTL(info.resource)
	if err != nil {
		return 0, err
	}
	if !ok || value != info.value {
		return 0, nil
	}
	return ttl, nil
}

func (r *redlock) RemainingTTLForResource(resource string) (int, error) {
	ok, _, ttl, err := r.TryGetRemainingTTL(resource)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return ttl, nil
}

func (r *redlock) IsLocked(resource string) (bool, error) {
	ttl, err := r.RemainingTTLForResource(resource)
	if err != nil {
		return false, err
	}

	return ttl > 0, nil
}

func (r *redlock) IsLockValid(info *LockInfo) (bool, error) {
	ttl, err := r.RemainingTTLForLock(info)
	if err != nil {
		return false, err
	}
	return ttl > 0, nil
}

func (r *redlock) ExtendLock(info *LockInfo, newTTL int) error {
	if info == nil {
		panic("nil LockInfo provided to ExtendLock")
	}

	newInfo := r.lockInstances(info.resource, info.value, newTTL)
	if newInfo == nil {
		return StaleLockError{}
	}

	info.ExpiresAt = newInfo.ExpiresAt
	info.Validity = newInfo.Validity

	return nil
}

func (r *redlock) TryGetRemainingTTL(resource string) (ok bool, value string, ttl int, err error) {
	allTTLs := map[string][]int{}
	thence := time.Now()
	for _, c := range r.clients {
		val, t, err := c.getRemainingTTL(resource)
		if err != nil {
			return false, "", 0, err
		}
		list, _ := allTTLs[val]
		list = append(list, t)
		allTTLs[val] = list
	}
	elapsed := time.Since(thence)

	authoritativeValue := ""
	maxValue := 0
	for k, v := range allTTLs {
		if len(v) > maxValue {
			authoritativeValue = k
			maxValue = len(v)
		}
	}
	ttls := allTTLs[authoritativeValue]

	if authoritativeValue == "" {
		return false, "", 0, nil
	}

	if len(ttls) > 0 && len(ttls) >= r.quorum {
		// Return the minimum TTL of an N/2+1 selection.
		sort.Ints(ttls)
		minTTL := ttls[len(ttls)-r.quorum:][0]
		minTTL = minTTL - int(elapsed.Seconds()) - r.drift(minTTL)
		return true, authoritativeValue, minTTL, nil
	}

	return false, "", 0, nil
}

func (r *redlock) tryLockInstances(resource string, value string, ttl int) *LockInfo {
	for i := 0; i < r.RetryCount; i++ {
		if i > 0 {
			time.Sleep(r.attemptRetryDelay(i))
		}
		lockInfo := r.lockInstances(resource, value, ttl)
		if lockInfo != nil {
			return lockInfo
		}
	}

	return nil
}

func (r *redlock) attemptRetryDelay(attempt int) time.Duration {
	var retryDelay time.Duration
	if r.RetryDelayFn != nil {
		retryDelay = r.RetryDelayFn(attempt)
	} else {
		retryDelay = r.RetryDelay
	}
	rand.Seed(time.Now().UnixNano())
	return time.Millisecond * time.Duration(retryDelay.Milliseconds()+int64(rand.Intn(r.RetryJitter)))
}

func (r *redlock) lockInstances(resource string, value string, ttl int) *LockInfo {
	thence := time.Now()
	locked := 0

	for _, c := range r.clients {
		if err := c.lock(resource, value, ttl); err == nil {
			locked++
		}
	}

	elapsed := time.Since(thence)
	validity := ttl - int(elapsed.Seconds()) - r.drift(ttl)

	if locked >= r.quorum && validity >= 0 {
		return &LockInfo{
			parent:    r,
			Validity:  validity,
			ExpiresAt: time.Now().Add(time.Second * time.Duration(validity)),
			resource:  resource,
			value:     value,
		}
	} else {
		for _, c := range r.clients {
			c.unlock(resource, value)
		}
	}

	return nil
}

func (r *redlock) drift(ttl int) int {
	return int(float64(ttl)*r.ClockDriftFactor) + 2
}
