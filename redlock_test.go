package redlock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func AssertNotLockable(t *testing.T, rl *Redlock, resource string) {
	locked, err := rl.IsLocked(resource)
	require.NoError(t, err)
	assert.True(t, locked, "Expected '"+resource+"' to be locked")
}

func RandomKey(t *testing.T) string {
	buf := make([]byte, 3)
	_, err := io.ReadFull(rand.Reader, buf)
	require.NoError(t, err)

	return hex.EncodeToString(buf)
}

func AutoUnlock(t *testing.T, info *LockInfo) {
	t.Cleanup(info.Unlock)
}

func MakeClient(t *testing.T) *Redlock {
	r := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	_, err := r.ScriptFlush(context.Background()).Result()
	require.NoError(t, err)

	t.Cleanup(func() { _ = r.Close() })

	return NewRedlock(r)
}

const ttl = 1000

func TestLock(t *testing.T) {
	cli := MakeClient(t)
	key := RandomKey(t)

	li, err := cli.Lock(key, ttl)
	require.NoError(t, err)
	require.NotNil(t, li)
	AutoUnlock(t, li)

	AssertNotLockable(t, cli, key)
}

func TestLockTTLAsMillis(t *testing.T) {
	cli := MakeClient(t)
	key := RandomKey(t)

	ttl := 20000

	li, err := cli.Lock(key, ttl)
	require.NoError(t, err)
	require.NotNil(t, li)
	AutoUnlock(t, li)

	pttl, err := cli.RemainingTTLForResource(key)
	require.NoError(t, err)

	assert.InDelta(t, ttl, pttl, 300)

	AssertNotLockable(t, cli, key)
}

func TestLockFailure(t *testing.T) {
	cli := MakeClient(t)
	key := RandomKey(t)
	{
		li, err := cli.Lock(key, ttl)
		require.NoError(t, err)
		require.NotNil(t, li)
		AutoUnlock(t, li)
	}

	t.Run("#lock returns an error", func(t *testing.T) {
		li, err := cli.Lock(key, ttl)
		require.ErrorAs(t, err, &LockError{})
		assert.True(t, IsLockError(err))
		require.Nil(t, li)
	})
}

func TestLockUnlock(t *testing.T) {
	cli := MakeClient(t)
	key := RandomKey(t)

	li, err := cli.Lock(key, ttl)
	require.NoError(t, err)
	li.Unlock()

	li, err = cli.Lock(key, ttl)
	require.NoError(t, err)
	li.Unlock()
}

func TestRemainingTTLForLock(t *testing.T) {
	cli := MakeClient(t)
	key := RandomKey(t)

	li, err := cli.Lock(key, ttl)
	require.NoError(t, err)
	AutoUnlock(t, li)
	rem, err := cli.RemainingTTLForLock(li)
	require.NoError(t, err)
	assert.InDelta(t, 1000, rem, 300)
}

func TestLockValid(t *testing.T) {
	cli := MakeClient(t)
	key := RandomKey(t)

	li, err := cli.Lock(key, 500)
	require.NoError(t, err)
	AutoUnlock(t, li)

	valid, err := cli.IsLockValid(li)
	require.NoError(t, err)
	locked, err := cli.IsLocked(key)
	require.NoError(t, err)

	assert.True(t, valid, "lock should be valid")
	assert.True(t, locked, "lock should be held")

	time.Sleep(500 * time.Millisecond)

	valid, err = cli.IsLockValid(li)
	require.NoError(t, err)
	locked, err = cli.IsLocked(key)
	require.NoError(t, err)

	assert.False(t, valid, "lock should be valid")
	assert.False(t, locked, "lock should be held")
}

func TestMultiLock(t *testing.T) {
	cli := MakeClient(t)
	key := RandomKey(t)

	locked := make(chan bool)
	go func() {
		li, err := cli.Lock(key, 5000)
		require.NoError(t, err)
		defer li.Unlock()
		locked <- true
		time.Sleep(200 * time.Millisecond)
	}()

	<-locked
	li, err := cli.Lock(key, 1000)
	require.NoError(t, err)
	li.Unlock()
}

func TestRace(t *testing.T) {
	cli := MakeClient(t)
	key := RandomKey(t)

	start := make(chan bool)
	wg := sync.WaitGroup{}

	concurrency := 3

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			li, err := cli.Lock(key, 5000)
			require.NoError(t, err)
			fmt.Printf("%d obtained lock at %s\n", i, time.Now())
			defer li.Unlock()
			time.Sleep(200 * time.Millisecond)
		}(i)
	}

	close(start)
	wg.Wait()
}

func TestWrapperLock(t *testing.T) {
	cli := MakeClient(t)
	key := RandomKey(t)

	wrapper := cli.clients[0]
	err := wrapper.lock(key, "test", 5000)
	require.NoError(t, err)

	err = wrapper.lock(key, "tests", 5000)
	assert.Equal(t, redis.Nil, err)
}

func TestExtendLock(t *testing.T) {
	cli := MakeClient(t)
	key := RandomKey(t)

	info, err := cli.Lock(key, 2000)
	require.NoError(t, err)
	defer AutoUnlock(t, info)

	valid, err := info.IsValid()
	require.NoError(t, err)
	assert.True(t, valid)

	err = cli.ExtendLock(info, 5000)
	require.NoError(t, err)

	ttl, err := cli.RemainingTTLForLock(info)
	require.NoError(t, err)
	require.InDelta(t, 5000, ttl, 300)
}

func TestExtendLockStale(t *testing.T) {
	cli := MakeClient(t)
	key := RandomKey(t)

	info, err := cli.Lock(key, 100)
	require.NoError(t, err)

	time.Sleep(200) // make sure acquired lock expires
	newInfo, err := cli.Lock(key, 1000)
	require.NoError(t, err)
	AutoUnlock(t, newInfo)

	err = cli.ExtendLock(info, 5000)
	require.ErrorAs(t, err, &StaleLockError{})
	assert.True(t, IsStaleError(err))
}

func TestLocking(t *testing.T) {
	cli := MakeClient(t)
	key := RandomKey(t)

	locked := make(chan bool)
	ok := make(chan bool)
	ok2 := make(chan bool)

	go func() {
		err := cli.Locking(key, 5000, func() error {
			close(locked)
			<-ok
			return nil
		})
		require.NoError(t, err)
		close(ok2)
	}()

	<-locked
	status, err := cli.IsLocked(key)
	require.NoError(t, err)
	require.True(t, status)
	close(ok)

	<-ok2
	status, err = cli.IsLocked(key)
	require.NoError(t, err)
	require.False(t, status)
}
