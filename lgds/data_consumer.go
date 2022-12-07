package lgds

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type UploadData struct {
	Data []Data `json:"data"`
}

type DataConsumer struct {
	serverUrl     string // 接收端地址
	appid         string
	ak            string
	sk            string
	timeout       time.Duration // 网络请求超时时间, 单位毫秒
	bufferMutex   *sync.RWMutex
	cacheMutex    *sync.RWMutex // 缓存锁
	buffer        []Data
	batchSize     int
	cacheBuffer   []Data // 缓存
	cacheCapacity int    // 缓存最大容量
	faildMutex    *sync.RWMutex
	faildBuffer   []Data //失败存储
}

type DataConfig struct {
	ServerUrl     string // 接收端地址
	AppId         string // 项目ID
	AccessKey     string // 用户名
	SecretKey     string // 秘钥
	BatchSize     int    // 上传数目
	Timeout       int    // 网络请求超时时间, 单位毫秒
	AutoFlush     bool   // 自动上传
	Interval      int    // 自动上传间隔，单位秒
	CacheCapacity int    // 缓存最大容量
}

const (
	DefaultTimeOut       = 30000 // 默认超时时长 30 秒
	DefaultBatchSize     = 100   // 默认批量发送条数
	MaxBatchSize         = 200   // 最大批量发送条数
	DefaultInterval      = 10    // 默认自动上传间隔 30 秒
	DefaultCacheCapacity = 200
)

// NewConsumer
//
//	@Description:
//	@param serverUrl 上报地址
//	@param appid 应用ID
//	@param ak AccessKey
//	@param sk SecretKey
//	@return Consumer 消费者
//	@return error 错误信息
func NewConsumer(serverUrl, appid, ak, sk string, autoFlush bool) (Consumer, error) {
	config := DataConfig{
		ServerUrl: serverUrl,
		AppId:     appid,
		AccessKey: ak,
		SecretKey: sk,
		AutoFlush: autoFlush,
	}
	return initDataConsumer(config)
}

func initDataConsumer(config DataConfig) (Consumer, error) {
	if config.ServerUrl == "" {
		return nil, errors.New(fmt.Sprint("ServerUrl 不能为空"))
	}
	u, err := url.Parse(config.ServerUrl)
	if err != nil {
		return nil, err
	}
	u.Path = "/logagent"

	var batchSize int
	if config.BatchSize > MaxBatchSize {
		batchSize = MaxBatchSize
	} else if config.BatchSize <= 0 {
		batchSize = DefaultBatchSize
	} else {
		batchSize = config.BatchSize
	}

	var cacheCapacity int
	if config.CacheCapacity <= 0 {
		cacheCapacity = DefaultCacheCapacity
	} else {
		cacheCapacity = config.CacheCapacity
	}

	var timeout int
	if config.Timeout == 0 {
		timeout = DefaultTimeOut
	} else {
		timeout = config.Timeout
	}
	c := &DataConsumer{
		serverUrl:     u.String(),
		appid:         config.AppId,
		ak:            config.AccessKey,
		sk:            config.SecretKey,
		timeout:       time.Duration(timeout) * time.Millisecond,
		bufferMutex:   new(sync.RWMutex),
		cacheMutex:    new(sync.RWMutex),
		batchSize:     batchSize,
		buffer:        make([]Data, 0, batchSize),
		cacheCapacity: cacheCapacity,
		cacheBuffer:   make([]Data, 0, cacheCapacity),
		faildMutex:    new(sync.RWMutex),
		faildBuffer:   make([]Data, 0),
	}

	var interval int
	if config.Interval == 0 {
		interval = DefaultInterval
	} else {
		interval = config.Interval
	}
	if config.AutoFlush {
		go func() {
			ticker := time.NewTicker(time.Duration(interval) * time.Second)
			defer ticker.Stop()
			for {
				<-ticker.C
				_ = c.Flush()
			}
		}()
	}
	return c, nil
}

func (c *DataConsumer) Add(d Data) error {
	c.bufferMutex.Lock()
	c.buffer = append(c.buffer, d)
	c.bufferMutex.Unlock()

	if c.getBufferLength() >= c.batchSize || c.getCacheLength() > 0 {
		err := c.Flush()
		return err
	}

	return nil
}

func (c *DataConsumer) Flush() error {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	c.bufferMutex.Lock()
	defer c.bufferMutex.Unlock()

	if len(c.buffer) == 0 && len(c.cacheBuffer) == 0 {
		return nil
	}

	defer func() {
		if len(c.cacheBuffer) > c.cacheCapacity {
			c.cacheBuffer = c.cacheBuffer[1:]
		}
	}()

	if len(c.cacheBuffer) == 0 || len(c.buffer) >= c.batchSize {
		for _, v := range c.buffer {
			c.cacheBuffer = append(c.cacheBuffer, v)
		}
		c.buffer = make([]Data, 0, c.batchSize)
	}
	err := c.uploadEvents()
	return err
}

func (c *DataConsumer) uploadEvents() error {
	buffers := make([]Data, len(c.cacheBuffer))
	//取出将要上传的数据
	copy(buffers[:], c.cacheBuffer)
	//清除缓存的数据
	c.cacheBuffer = make([]Data, 0, c.cacheCapacity)
	data, err := json.Marshal(UploadData{Data: buffers})
	if err != nil {
		return err
	}
	go func() {
		code, message, err := c.send(data)
		if code == 200 {
			log.Println("上报成功")
		} else {
			c.writeFaildData(buffers)
			log.Printf("信息:%v,报错:%v", message, err)
		}
	}()
	return nil
}

func (c *DataConsumer) FlushAll() error {
	for c.getCacheLength() > 0 || c.getBufferLength() > 0 {
		if err := c.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (c *DataConsumer) Close() error {
	return c.FlushAll()
}

//上报数据

func (c *DataConsumer) send(data []byte) (statusCode int, Msg string, err error) {
	postData := bytes.NewBuffer(data)
	var resp *http.Response
	salt := RandString(25)
	req, _ := http.NewRequest("POST", c.serverUrl, postData)
	req.Header.Set("user-agent", "lgds-go-sdk")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("version", SdkVersion)
	req.Header.Set("lib", LibName)
	req.Header.Set("Salt", salt)
	req.Header.Set("AppId", c.appid)
	req.Header.Set("Signature", Sha256EnCode(fmt.Sprintf("%s%s%s%s", c.ak, c.sk, salt, GetUTC())))
	client := &http.Client{Timeout: c.timeout}
	resp, err = client.Do(req)
	if err != nil {
		return 500, "HTTP上报失败", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return resp.StatusCode, "success", nil
	}
	body, _ := io.ReadAll(resp.Body)
	var result struct {
		Message string `json:"message"`
	}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return resp.StatusCode, "", err
	}
	return resp.StatusCode, result.Message, nil

}

func (c *DataConsumer) getBufferLength() int {
	c.bufferMutex.RLock()
	defer c.bufferMutex.RUnlock()
	return len(c.buffer)
}

func (c *DataConsumer) getCacheLength() int {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()
	return len(c.cacheBuffer)
}

//写入失败信息

func (c *DataConsumer) writeFaildData(data []Data) {
	c.faildMutex.Lock()
	for _, v := range data {
		c.faildBuffer = append(c.cacheBuffer, v)
	}
	c.faildMutex.Unlock()
}
