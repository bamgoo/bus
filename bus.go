package bus

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/bamgoo/bamgoo"
	base "github.com/bamgoo/base"
	"github.com/bamgoo/util"
)

var (
	errBusNotReady = errors.New("bus is not ready")
)

var (
	module = &busModule{
		drivers:     make(map[string]Driver, 0),
		configs:     make(map[string]Config, 0),
		connections: make(map[string]Connection, 0),
		weights:     make(map[string]int, 0),
		services:    make(map[string]struct{}, 0),
	}
	host = bamgoo.Mount(module)
)

type (
	// Handler processes incoming payload and returns reply bytes for call.
	Handler func([]byte) ([]byte, error)

	// Driver connections a bus transport.
	Driver interface {
		Connect(*Instance) (Connection, error)
	}

	// Connection defines a bus transport connection.
	Connection interface {
		Open() error
		Close() error
		Start() error
		Stop() error

		Register(subject string) error

		Request(subject string, data []byte, timeout time.Duration) ([]byte, error)
		Publish(subject string, data []byte) error
		Enqueue(subject string, data []byte) error

		Stats() []bamgoo.ServiceStats
	}

	busModule struct {
		mutex sync.RWMutex

		drivers     map[string]Driver
		configs     map[string]Config
		connections map[string]Connection
		weights     map[string]int
		wrr         *util.WRR
		services    map[string]struct{}

		opened  bool
		started bool
	}

	Instance struct {
		Name   string
		Config Config
	}

	Config struct {
		Driver  string
		Weight  int
		Prefix  string
		Setting base.Map
	}

	Configs map[string]Config
)

const (
	subjectCall  = "call"
	subjectQueue = "queue"
	subjectEvent = "event"
)

type (
	// busRequest combines metadata and payload for transmission.
	busRequest struct {
		bamgoo.Metadata
		Name    string   `json:"name"`
		Payload base.Map `json:"payload,omitempty"`
	}

	// busResponse contains result with full Res info.
	busResponse struct {
		Code  int    `json:"code"`
		State string `json:"state"`
		Desc  string `json:"desc,omitempty"`
		Time  int64  `json:"time"`
		Data  base.Map
	}
)

// Register dispatches registrations.
func (m *busModule) Register(name string, value base.Any) {
	switch v := value.(type) {
	case Driver:
		m.RegisterDriver(name, v)
	case Config:
		m.RegisterConfig(name, v)
	case Configs:
		m.RegisterConfigs(v)
	case bamgoo.Service:
		m.RegisterService(name)
	}
}

// RegisterDriver registers a bus driver.
func (m *busModule) RegisterDriver(name string, driver Driver) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if name == "" {
		name = bamgoo.DEFAULT
	}
	if driver == nil {
		panic("Invalid bus driver: " + name)
	}
	if _, ok := m.drivers[name]; ok {
		panic("Bus driver already registered: " + name)
	}
	m.drivers[name] = driver
}

// RegisterConfig registers a named bus config.
// If name is empty, it uses DEFAULT.
func (m *busModule) RegisterConfig(name string, cfg Config) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened || m.started {
		return
	}

	if name == "" {
		name = bamgoo.DEFAULT
	}
	if _, ok := m.configs[name]; ok {
		panic("Bus config already registered: " + name)
	}
	m.configs[name] = cfg
}

// RegisterConfigs registers multiple named bus configs.
func (m *busModule) RegisterConfigs(configs Configs) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened || m.started {
		return
	}

	for name, cfg := range configs {
		if name == "" {
			name = bamgoo.DEFAULT
		}
		if _, ok := m.configs[name]; ok {
			panic("Bus config already registered: " + name)
		}
		m.configs[name] = cfg
	}
}

// RegisterService binds service name into bus subjects.
func (m *busModule) RegisterService(name string) {
	if name == "" {
		return
	}
	m.mutex.Lock()
	m.services[name] = struct{}{}
	m.mutex.Unlock()
}

func (m *busModule) Config(global base.Map) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened || m.started {
		return
	}

	cfgAny, ok := global["bus"]
	if !ok {
		return
	}
	cfgMap, ok := castToMap(cfgAny)
	if !ok || cfgMap == nil {
		return
	}

	rootConfig := base.Map{}
	for key, val := range cfgMap {
		if conf, ok := castToMap(val); ok && key != "setting" {
			m.configure(key, conf)
		} else {
			rootConfig[key] = val
		}
	}
	if len(rootConfig) > 0 {
		m.configure(bamgoo.DEFAULT, rootConfig)
	}
}

func (m *busModule) configure(name string, conf base.Map) {
	cfg := Config{}
	if existing, ok := m.configs[name]; ok {
		cfg = existing
	}

	if v, ok := conf["driver"].(string); ok && v != "" {
		cfg.Driver = v
	}
	if v, ok := conf["prefix"].(string); ok {
		cfg.Prefix = v
	}
	if v, ok := parseWeight(conf["weight"]); ok {
		cfg.Weight = v
	}
	if v, ok := castToMap(conf["setting"]); ok {
		cfg.Setting = v
	}

	m.configs[name] = cfg
}

// Setup initializes defaults.
func (m *busModule) Setup() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened || m.started {
		return
	}

	if len(m.configs) == 0 {
		m.configs[bamgoo.DEFAULT] = Config{Driver: bamgoo.DEFAULT, Weight: 1}
	}

	// normalize configs
	for name, cfg := range m.configs {
		if name == "" {
			name = bamgoo.DEFAULT
		}
		if cfg.Driver == "" {
			cfg.Driver = bamgoo.DEFAULT
		}
		if cfg.Weight == 0 {
			cfg.Weight = 1
		}
		m.configs[name] = cfg
	}
}

// Open connections bus and registers services.
func (m *busModule) Open() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened {
		return
	}

	if len(m.configs) == 0 {
		panic("Missing bus config")
	}

	for name, cfg := range m.configs {
		driver, ok := m.drivers[cfg.Driver]
		if !ok || driver == nil {
			panic("Missing bus driver: " + cfg.Driver)
		}

		if cfg.Weight == 0 {
			cfg.Weight = 1
		}

		inst := &Instance{Name: name, Config: cfg}
		conn, err := driver.Connect(inst)
		if err != nil {
			panic("Failed to connect to bus: " + err.Error())
		}
		if err := conn.Open(); err != nil {
			panic("Failed to open bus: " + err.Error())
		}

		for svc := range m.services {
			base := m.subjectBase(cfg.Prefix, svc)
			if err := conn.Register(base); err != nil {
				panic("Failed to register bus: " + err.Error())
			}
		}

		m.connections[name] = conn
		m.weights[name] = cfg.Weight
	}

	m.wrr = util.NewWRR(m.weights)
	m.opened = true
}

// Start launches bus subscriptions.
func (m *busModule) Start() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.started {
		return
	}

	if len(m.connections) == 0 {
		panic("Bus not opened")
	}

	for _, conn := range m.connections {
		_ = conn.Start()
	}

	fmt.Println("bus module is running.")

	m.started = true
}

// Stop terminates bus subscriptions.
func (m *busModule) Stop() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if !m.started {
		return
	}

	for _, conn := range m.connections {
		_ = conn.Stop()
	}

	m.started = false
}

// Close closes bus connections.
func (m *busModule) Close() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if !m.opened {
		return
	}

	for _, conn := range m.connections {
		conn.Close()
	}

	m.connections = make(map[string]Connection, 0)
	m.weights = make(map[string]int, 0)
	m.wrr = nil
	m.opened = false
}

func (m *busModule) subject(prefix, kind, name string) string {
	if prefix == "" {
		return kind + "." + name
	}
	return prefix + kind + "." + name
}

func (m *busModule) subjectBase(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + name
}

func (m *busModule) pick() (Connection, string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.wrr == nil {
		return nil, ""
	}
	name := m.wrr.Next()
	if name == "" {
		return nil, ""
	}
	conn := m.connections[name]
	cfg := m.configs[name]
	return conn, cfg.Prefix
}

// Request sends a request and waits for reply.
func (m *busModule) Request(meta *bamgoo.Meta, name string, value base.Map, timeout time.Duration) (base.Map, base.Res) {
	conn, prefix := m.pick()

	if conn == nil {
		return nil, bamgoo.ErrorResult(errBusNotReady)
	}

	data, err := encodeRequest(meta, name, value)
	if err != nil {
		return nil, bamgoo.ErrorResult(err)
	}

	baseName := m.subjectBase(prefix, name)
	subject := m.subject("", subjectCall, baseName)
	resBytes, err := conn.Request(subject, data, timeout)
	if err != nil {
		return nil, bamgoo.ErrorResult(err)
	}

	return decodeResponse(resBytes)
}

// Publish broadcasts an event to all subscribers.
func (m *busModule) Publish(meta *bamgoo.Meta, name string, value base.Map) error {
	conn, prefix := m.pick()

	if conn == nil {
		return errBusNotReady
	}

	data, err := encodeRequest(meta, name, value)
	if err != nil {
		return err
	}

	baseName := m.subjectBase(prefix, name)
	subject := m.subject("", subjectEvent, baseName)
	return conn.Publish(subject, data)
}

// Enqueue sends to a queue (one subscriber receives).
func (m *busModule) Enqueue(meta *bamgoo.Meta, name string, value base.Map) error {
	conn, prefix := m.pick()

	if conn == nil {
		return errBusNotReady
	}

	data, err := encodeRequest(meta, name, value)
	if err != nil {
		return err
	}

	baseName := m.subjectBase(prefix, name)
	subject := m.subject("", subjectQueue, baseName)
	return conn.Enqueue(subject, data)
}

func encodeRequest(meta *bamgoo.Meta, name string, payload base.Map) ([]byte, error) {
	req := busRequest{
		Name:    name,
		Payload: payload,
	}
	if meta != nil {
		req.Metadata = meta.Metadata()
	}
	return json.Marshal(req)
}

func decodeRequest(data []byte) (*bamgoo.Meta, string, base.Map, error) {
	var req busRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, "", nil, err
	}

	meta := bamgoo.NewMeta()
	meta.Metadata(req.Metadata)

	if req.Payload == nil {
		req.Payload = base.Map{}
	}
	return meta, req.Name, req.Payload, nil
}

func encodeResponse(data base.Map, res base.Res) ([]byte, error) {
	if res == nil {
		res = bamgoo.OK
	}
	resp := busResponse{
		Code:  res.Code(),
		State: res.State(),
		Desc:  res.Error(),
		Time:  time.Now().UnixMilli(),
		Data:  data,
	}
	return json.Marshal(resp)
}

func decodeResponse(data []byte) (base.Map, base.Res) {
	var resp busResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, bamgoo.ErrorResult(err)
	}

	res := bamgoo.Result(resp.Code, resp.State, resp.Desc)
	if resp.Data == nil {
		resp.Data = base.Map{}
	}
	return resp.Data, res
}

// HandleCall handles request/reply for a bus instance.
func (inst *Instance) HandleCall(data []byte) ([]byte, error) {
	meta, name, payload, err := decodeRequest(data)
	if err != nil {
		return nil, err
	}

	body, res, _ := host.InvokeLocal(meta, name, payload)
	return encodeResponse(body, res)
}

// HandleAsync handles async execution (queue/event) for a bus instance.
func (inst *Instance) HandleAsync(data []byte) error {
	meta, name, payload, err := decodeRequest(data)
	if err != nil {
		return err
	}

	go host.InvokeLocal(meta, name, payload)
	return nil
}

// Stats returns service statistics from all connections.
func (m *busModule) Stats() []bamgoo.ServiceStats {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var all []bamgoo.ServiceStats
	for _, conn := range m.connections {
		stats := conn.Stats()
		if stats != nil {
			all = append(all, stats...)
		}
	}
	return all
}

func castToMap(value base.Any) (base.Map, bool) {
	switch v := value.(type) {
	case base.Map:
		return v, true
	default:
		return nil, false
	}
}

func parseWeight(value base.Any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case string:
		n, err := strconv.Atoi(v)
		if err == nil {
			return n, true
		}
	}
	return 0, false
}
