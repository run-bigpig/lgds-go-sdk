package lgds

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"time"
)

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	DATE_FORMAT   = "2006-01-02 15:04:05"
	minute_format = "2006-01-02 15:04"
	KEY_PATTERN   = "^[a-zA-Z#][A-Za-z0-9_]{0,49}$"
)

var keyPattern, _ = regexp.Compile(KEY_PATTERN)

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

func parseTime(input []byte) string {
	var re = regexp.MustCompile(`(((\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.(\d{3}))\d+)?)(Z|[\+-]\d{2}:\d{2})`)
	var substitution = "$3 $4.$5"
	for re.Match(input) {
		input = re.ReplaceAll(input, []byte(substitution))
	}
	return string(input)
}

func CleanSpaces(str string) string {
	str = strings.Replace(str, " ", "", -1)
	str = strings.Replace(str, "\n", "", -1)
	str = strings.Replace(str, "\r", "", -1)
	return str
}

func Md5(data []byte) string {
	s := fmt.Sprintf("%x", md5.Sum(data))
	return s
}

func Base64Encode(data string) string {
	sEnc := base64.StdEncoding.EncodeToString([]byte(data))
	return sEnc
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
	return time.Now().UTC().Format(minute_format)
}
