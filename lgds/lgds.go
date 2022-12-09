package lgds

import (
	"errors"
	"sync"
	"time"
)

const (
	Track      = "track"
	User       = "user"
	SdkVersion = "1.0.0"
	LibName    = "Golang"
)

// Data 数据信息
type Data struct {
	DeviceId   string                 `json:"#device_id"`
	UserId     string                 `json:"#user_id"`
	AppName    string                 `json:"#app_name"`
	Platform   string                 `json:"#platform"`
	Server     int                    `json:"#server"`
	Type       string                 `json:"#type"`
	Action     string                 `json:"#action"`
	Time       interface{}            `json:"#time"`
	EventName  string                 `json:"#event_name"`
	Properties map[string]interface{} `json:"#properties"`
}

// Consumer 为数据实现 IO 操作（写入磁盘或者发送到接收端）
type Consumer interface {
	Add(d Data) error
	Flush() error
	Close() error
}

type LGDS struct {
	consumer        Consumer
	superProperties map[string]interface{} //公共属性
	mutex           *sync.RWMutex
}

// New 初始化 LGDS
func New(c Consumer) LGDS {
	return LGDS{
		consumer:        c,
		superProperties: make(map[string]interface{}),
		mutex:           new(sync.RWMutex)}
}

// GetSuperProperties 返回公共事件属性
func (l *LGDS) GetSuperProperties() map[string]interface{} {
	result := make(map[string]interface{})
	l.mutex.RLock()
	mergeProperties(result, l.superProperties)
	l.mutex.RUnlock()
	return result
}

// SetSuperProperties 设置公共事件属性
func (l *LGDS) SetSuperProperties(superProperties map[string]interface{}) {
	l.mutex.Lock()
	mergeProperties(l.superProperties, superProperties)
	l.mutex.Unlock()
}

// ClearSuperProperties 清除公共事件属性
func (l *LGDS) ClearSuperProperties() {
	l.mutex.Lock()
	l.superProperties = make(map[string]interface{})
	l.mutex.Unlock()
}

//追踪一个用户事件

func (l *LGDS) Track(DeviceId, UserId, AppName, Platform, EventName string, Server int, properties map[string]interface{}) error {
	if len(EventName) == 0 {
		return errors.New("the event name must be provided")
	}
	// 获取设置的公共属性
	p := l.GetSuperProperties()

	mergeProperties(p, properties)

	return l.add(DeviceId, UserId, AppName, Platform, Server, EventName, Track, "insert", p)
}

// 追踪一个用户注册

func (l *LGDS) User(DeviceId string, UserId string, AppName string, Platform string, Server int, properties map[string]interface{}) error {
	if properties == nil {
		return errors.New("invalid params for properties is nil")
	}
	p := make(map[string]interface{})
	mergeProperties(p, properties)
	return l.add(DeviceId, UserId, AppName, Platform, Server, User, User, "insert", p)
}

// 追踪一个用户属性更新
func (l *LGDS) UserUpdate(DeviceId string, UserId string, AppName string, Platform string, Server int, properties map[string]interface{}) error {
	if properties == nil {
		return errors.New("invalid params for properties is nil")
	}
	p := make(map[string]interface{})
	mergeProperties(p, properties)
	return l.add(DeviceId, UserId, AppName, Platform, Server, User, User, "update", p)
}

// Flush 立即开始数据 IO 操作
func (l *LGDS) Flush() error {
	return l.consumer.Flush()
}

// Close 关闭 LGDS
func (l *LGDS) Close() error {
	return l.consumer.Close()
}

func (l *LGDS) add(DeviceId string, UserId string, AppName string, Platform string, Server int, EventName, DataType string, Action string, properties map[string]interface{}) error {
	if len(DeviceId) == 0 && len(UserId) == 0 {
		return errors.New("invalid paramters: device_id and user_id cannot be empty at the same time")
	}
	data := Data{
		DeviceId:   DeviceId,
		UserId:     DeviceId,
		AppName:    AppName,
		Platform:   Platform,
		Time:       time.Now().UTC().Format(DateFormat),
		EventName:  EventName,
		Type:       DataType,
		Server:     Server,
		Action:     Action,
		Properties: properties,
	}

	// 检查数据格式, 并将时间类型数据转为符合格式要求的字符串
	err := formatProperties(&data, properties)
	if err != nil {
		return err
	}

	return l.consumer.Add(data)
}
