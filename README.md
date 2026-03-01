# bus

`bus` 是 infrago 的模块包。

## 安装

```bash
go get github.com/infrago/bus@latest
```

## 最小接入

```go
package main

import (
    _ "github.com/infrago/bus"
    "github.com/infrago/infra"
)

func main() {
    infra.Run()
}
```

## 配置示例

```toml
[bus]
driver = "default"
```

## 公开 API（摘自源码）

- `func (m *busModule) Register(name string, value base.Any)`
- `func (m *busModule) RegisterDriver(name string, driver Driver)`
- `func (m *busModule) RegisterConfig(name string, cfg Config)`
- `func (m *busModule) RegisterConfigs(configs Configs)`
- `func (m *busModule) RegisterService(name string, svc infra.Service)`
- `func (m *busModule) Config(global base.Map)`
- `func (m *busModule) Setup()`
- `func (m *busModule) Open()`
- `func (m *busModule) Start()`
- `func (m *busModule) Stop()`
- `func (m *busModule) Close()`
- `func (m *busModule) Request(meta *infra.Meta, name string, value base.Map, timeout time.Duration) (base.Map, base.Res)`
- `func (m *busModule) Broadcast(meta *infra.Meta, name string, value base.Map) error`
- `func (m *busModule) Publish(meta *infra.Meta, name string, value base.Map) error`
- `func (m *busModule) Enqueue(meta *infra.Meta, name string, value base.Map) error`
- `func (inst *Instance) HandleCall(data []byte) ([]byte, error)`
- `func (inst *Instance) HandleAsync(data []byte) error`
- `func (m *busModule) Stats() []infra.ServiceStats`
- `func (m *busModule) ListNodes() []infra.NodeInfo`
- `func (m *busModule) ListServices() []infra.ServiceInfo`
- `func Stats() []infra.ServiceStats`
- `func ListNodes() []infra.NodeInfo`
- `func ListServices() []infra.ServiceInfo`
- `func (driver *defaultBusDriver) Connect(inst *Instance) (Connection, error)`
- `func (c *defaultBusConnection) Open() error  { return nil }`
- `func (c *defaultBusConnection) Close() error { return nil }`
- `func (c *defaultBusConnection) Start() error`
- `func (c *defaultBusConnection) Stop() error`
- `func (c *defaultBusConnection) Register(subject string) error`
- `func (c *defaultBusConnection) Request(_ string, data []byte, _ time.Duration) ([]byte, error)`
- `func (c *defaultBusConnection) Publish(_ string, data []byte) error`
- `func (c *defaultBusConnection) Enqueue(_ string, data []byte) error`
- `func (c *defaultBusConnection) Stats() []infra.ServiceStats`
- `func (c *defaultBusConnection) ListNodes() []infra.NodeInfo`
- `func (c *defaultBusConnection) ListServices() []infra.ServiceInfo`

## 排错

- 模块未运行：确认空导入已存在
- driver 无效：确认驱动包已引入
- 配置不生效：检查配置段名是否为 `[bus]`
