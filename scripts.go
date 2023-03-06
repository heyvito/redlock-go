package redlock

import (
	"crypto/sha1"
	"encoding/hex"
)

const unlockScript = `if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
else
	return 0
end`

const lockScript = `if (redis.call("exists", KEYS[1]) == 0) or redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("set", KEYS[1], ARGV[1], "PX", ARGV[2])
end`

const pttlScript = `return { redis.call("get", KEYS[1]), redis.call("pttl", KEYS[1]) }`

var (
	unlockScriptSHA = shasum([]byte(unlockScript))
	lockScriptSHA   = shasum([]byte(lockScript))
	pttlScriptSHA   = shasum([]byte(pttlScript))
)

func shasum(data []byte) string {
	sha := sha1.New()
	sha.Write(data)
	return hex.EncodeToString(sha.Sum(nil))
}
