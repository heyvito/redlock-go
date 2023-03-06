package redlock

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type redisWrapper struct {
	*redis.Client
}

func (r *redisWrapper) lock(resource string, val string, ttl int) error {
	return r.recoverScriptFlush(func() error {
		return r.EvalSha(context.Background(), lockScriptSHA, []string{resource}, val, ttl).Err()
	})
}

func (r *redisWrapper) unlock(resource string, val string) {
	_ = r.recoverScriptFlush(func() error {
		return r.EvalSha(context.Background(), unlockScriptSHA, []string{resource}, val).Err()
	})
}

func (r *redisWrapper) recoverScriptFlush(fn func() error) error {
	retryNoScript := true
	for {
		err := fn()
		if err != nil && redis.HasErrorPrefix(err, "NOSCRIPT") && retryNoScript {
			retryNoScript = false
			if installErr := r.installScripts(); installErr != nil {
				return fmt.Errorf("failed installing scripts: %w", installErr)
			}
			continue
		} else {
			return err
		}
	}
}

func (r *redisWrapper) installScripts() error {
	scripts := []string{unlockScript, lockScript, pttlScript}
	for _, s := range scripts {
		if err := r.ScriptLoad(context.Background(), s).Err(); err != nil {
			return err
		}
	}

	return nil
}

func (r *redisWrapper) getRemainingTTL(resource string) (string, int, error) {
	var val string
	var ttl int

	err := r.recoverScriptFlush(func() error {
		v, err := r.EvalSha(context.Background(), pttlScriptSHA, []string{resource}).Result()
		if err != nil {
			return err
		}

		tuple := v.([]any)
		if tuple[0] == nil {
			return nil
		}

		val = tuple[0].(string)
		ttl = int(tuple[1].(int64))
		return nil
	})
	if err != nil {
		return "", 0, err
	}

	return val, ttl, nil
}
