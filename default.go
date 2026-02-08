package bus

import (
	"errors"
	"sync"
	"time"

	"github.com/bamgoo/bamgoo"
)

func init() {
	bamgoo.Register(bamgoo.DEFAULT, &defaultBusDriver{})
}

var (
	errBusRunning       = errors.New("bus is running")
	errBusNotRunning    = errors.New("bus is not running")
	errBusInvalidTarget = errors.New("invalid bus target")
)

type (
	defaultBusDriver struct{}

	defaultBusConnection struct {
		mutex    sync.RWMutex
		running  bool
		services map[string]struct{}
		instance *Instance
	}
)

// Connect establishes an in-memory
func (driver *defaultBusDriver) Connect(inst *Instance) (Connection, error) {
	return &defaultBusConnection{
		services: make(map[string]struct{}, 0),
		instance: inst,
	}, nil
}

func (c *defaultBusConnection) Open() error  { return nil }
func (c *defaultBusConnection) Close() error { return nil }

func (c *defaultBusConnection) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.running {
		return errBusRunning
	}

	c.running = true
	return nil
}

func (c *defaultBusConnection) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.running {
		return errBusNotRunning
	}

	c.running = false
	return nil
}

// Register registers a service subject for local handling.
func (c *defaultBusConnection) Register(subject string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if subject == "" {
		return errBusInvalidTarget
	}

	c.services[subject] = struct{}{}

	return nil
}

// Request handles synchronous call - for in-memory bus, directly invoke local.
func (c *defaultBusConnection) Request(_ string, data []byte, _ time.Duration) ([]byte, error) {
	if c.instance == nil {
		c.instance = &Instance{}
	}
	return c.instance.HandleCall(data)
}

// Publish broadcasts event to all local handlers - for in-memory, invoke local.
func (c *defaultBusConnection) Publish(_ string, data []byte) error {
	if c.instance == nil {
		c.instance = &Instance{}
	}
	return c.instance.HandleAsync(data)
}

// Enqueue handles queued call - for in-memory bus, directly invoke local.
func (c *defaultBusConnection) Enqueue(_ string, data []byte) error {
	if c.instance == nil {
		c.instance = &Instance{}
	}
	return c.instance.HandleAsync(data)
}

// Stats returns empty stats for in-memory
func (c *defaultBusConnection) Stats() []bamgoo.ServiceStats {
	return nil
}
