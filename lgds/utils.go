package lgds

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math/rand"
	"regexp"
	"time"
)

const (
	letterBytes  = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	DateFormat   = "2006-01-02 15:04:05"
	minuteFormat = "2006-01-02 15:04"
	KeyPattern   = "^[a-zA-Z#][A-Za-z0-9_]{0,49}$"
)

var keyPattern, _ = regexp.Compile(KeyPattern)

func mergeProperties(target, source map[string]interface{}) {
	for k, v := range source {
		target[k] = v
	}
}

func formatProperties(d *Data, properties map[string]interface{}) error {

	if d.EventName != "" {
		matched := checkPattern([]byte(d.EventName))
		if !matched {
			return errors.New("Invalid event name: " + d.EventName)
		}
	}

	if properties != nil {
		for k, _ := range properties {
			isMatch := checkPattern([]byte(k))
			if !isMatch {
				return errors.New("Invalid property key: " + k)
			}
		}
	}
	return nil
}

func checkPattern(name []byte) bool {
	return keyPattern.Match(name)
}

func Sha256EnCode(encodestr string) string {
	h := sha256.New()
	h.Write([]byte(encodestr))
	return hex.EncodeToString(h.Sum(nil))
}

func RandString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func GetUTC() string {
	return time.Now().UTC().Format(minuteFormat)
}
