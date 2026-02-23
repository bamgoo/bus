package bus

import (
	"errors"
	"sort"
	"strings"
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
		prefix   string
	}
)

// Connect establishes an in-memory
func (driver *defaultBusDriver) Connect(inst *Instance) (Connection, error) {
	return &defaultBusConnection{
		services: make(map[string]struct{}, 0),
		instance: inst,
		prefix:   inst.Config.Prefix,
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

func (c *defaultBusConnection) ListNodes() []bamgoo.NodeInfo {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	identity := bamgoo.Identity()
	names := make([]string, 0, len(c.services))
	for name := range c.services {
		names = append(names, c.serviceName(name))
	}
	sort.Strings(names)

	return []bamgoo.NodeInfo{
		{
			Project:  bamgoo.Project(),
			Node:     identity.Node,
			Role:     identity.Role,
			Services: names,
			Updated:  time.Now().UnixMilli(),
		},
	}
}

func (c *defaultBusConnection) ListServices() []bamgoo.ServiceInfo {
	nodes := c.ListNodes()
	if len(nodes) == 0 {
		return nil
	}
	merged := make(map[string]*bamgoo.ServiceInfo)
	for _, node := range nodes {
		for _, svc := range node.Services {
			svcKey := svc
			info, ok := merged[svcKey]
			if !ok {
				info = &bamgoo.ServiceInfo{Service: svc, Name: svc}
				merged[svcKey] = info
			}
			info.Nodes = append(info.Nodes, bamgoo.ServiceNode{
				Node: node.Node,
				Role: node.Role,
			})
			if node.Updated > info.Updated {
				info.Updated = node.Updated
			}
		}
	}

	out := make([]bamgoo.ServiceInfo, 0, len(merged))
	for _, info := range merged {
		info.Instances = len(info.Nodes)
		out = append(out, *info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Service < out[j].Service })
	return out
}

func (c *defaultBusConnection) serviceName(subject string) string {
	if c.prefix == "" {
		return subject
	}
	return strings.TrimPrefix(subject, c.prefix)
}
