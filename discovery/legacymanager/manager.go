// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package legacymanager

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/targetgroup"
)

var (
	failedConfigs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "prometheus_sd_failed_configs",
			Help: "Current number of service discovery configurations that failed to load.",
		},
		[]string{"name"},
	)
	discoveredTargets = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "prometheus_sd_discovered_targets",
			Help: "Current number of discovered targets.",
		},
		[]string{"name", "config"},
	)
	receivedUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_sd_received_updates_total",
			Help: "Total number of update events received from the SD providers.",
		},
		[]string{"name"},
	)
	delayedUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_sd_updates_delayed_total",
			Help: "Total number of update events that couldn't be sent immediately.",
		},
		[]string{"name"},
	)
	sentUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_sd_updates_total",
			Help: "Total number of update events sent to the SD consumers.",
		},
		[]string{"name"},
	)
)

func RegisterMetrics() {
	prometheus.MustRegister(failedConfigs, discoveredTargets, receivedUpdates, delayedUpdates, sentUpdates)
}

type poolKey struct {
	setName  string
	provider string
}

// provider holds a Discoverer instance, its configuration and its subscribers.
type provider struct {
	name   string // provider名称，格式：fmt.Sprintf("%s/%d", typ, len(m.providers))
	d      discovery.Discoverer
	subs   []string // string切片，存放job名称，因为可能不同job下存在一致的服务发现配置，就只会生成一个provider，然后subs存放job列表；
	config interface{}
}

// NewManager is the Discovery Manager constructor.
func NewManager(ctx context.Context, logger log.Logger, options ...func(*Manager)) *Manager {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	mgr := &Manager{
		logger:         logger,
		syncCh:         make(chan map[string][]*targetgroup.Group),
		targets:        make(map[poolKey]map[string]*targetgroup.Group),
		discoverCancel: []context.CancelFunc{},
		ctx:            ctx,
		updatert:       5 * time.Second,
		triggerSend:    make(chan struct{}, 1),
	}
	for _, option := range options {
		option(mgr)
	}
	return mgr
}

// Name sets the name of the manager.
func Name(n string) func(*Manager) {
	return func(m *Manager) {
		m.mtx.Lock()
		defer m.mtx.Unlock()
		m.name = n
	}
}

// Manager maintains a set of discovery providers and sends each update to a map channel.
// Targets are grouped by the target set name.
type Manager struct {
	logger         log.Logger
	name           string
	mtx            sync.RWMutex
	ctx            context.Context
	discoverCancel []context.CancelFunc

	// Some Discoverers(eg. k8s) send only the updates for a given target group
	// so we use map[tg.Source]*targetgroup.Group to know which group to update.
	targets map[poolKey]map[string]*targetgroup.Group
	// providers keeps track of SD providers.
	providers []*provider
	// The sync channel sends the updates as a map where the key is the job value from the scrape config.
	syncCh chan map[string][]*targetgroup.Group

	// How long to wait before sending updates to the channel. The variable
	// should only be modified in unit tests.
	updatert time.Duration

	// The triggerSend channel signals to the manager that new updates have been received from providers.
	triggerSend chan struct{}
}

// Run starts the background processing
func (m *Manager) Run() error {
	go m.sender()
	for range m.ctx.Done() {
		m.cancelDiscoverers()
		return m.ctx.Err()
	}
	return nil
}

// SyncCh returns a read only channel used by all the clients to receive target updates.
func (m *Manager) SyncCh() <-chan map[string][]*targetgroup.Group {
	return m.syncCh
}

// ApplyConfig removes all running discovery providers and starts new ones using the provided config.
func (m *Manager) ApplyConfig(cfg map[string]discovery.Configs) error {
	// 加锁
	m.mtx.Lock()
	// 函数结束后 解锁
	defer m.mtx.Unlock()

	// 遍历已存在的target
	for pk := range m.targets {
		if _, ok := cfg[pk.setName]; !ok {
			// 删除标签
			discoveredTargets.DeleteLabelValues(m.name, pk.setName)
		}
	}
	// 取消所有Discoverer
	m.cancelDiscoverers()
	m.targets = make(map[poolKey]map[string]*targetgroup.Group)
	m.providers = nil
	m.discoverCancel = nil

	failedCount := 0
	for name, scfg := range cfg {
		// 根据scfg，注册服务发现实例
		failedCount += m.registerProviders(scfg, name)
		// 设置标签
		discoveredTargets.WithLabelValues(m.name, name).Set(0)
	}
	failedConfigs.WithLabelValues(m.name).Set(float64(failedCount))

	for _, prov := range m.providers {
		// 启动服务发现实例
		m.startProvider(m.ctx, prov)
	}

	return nil
}

// StartCustomProvider is used for sdtool. Only use this if you know what you're doing.
func (m *Manager) StartCustomProvider(ctx context.Context, name string, worker discovery.Discoverer) {
	p := &provider{
		name: name,
		d:    worker,
		subs: []string{name},
	}
	m.providers = append(m.providers, p)
	m.startProvider(ctx, p)
}

func (m *Manager) startProvider(ctx context.Context, p *provider) {
	level.Debug(m.logger).Log("msg", "Starting provider", "provider", p.name, "subs", fmt.Sprintf("%v", p.subs))
	ctx, cancel := context.WithCancel(ctx)
	// 记录发现的服务
	updates := make(chan []*targetgroup.Group)
	// 添加取消方法
	m.discoverCancel = append(m.discoverCancel, cancel)
	// 执行run  每个服务发现都有自己的run方法。
	// 这里是给服务发现 往updates这个channel中传数据
	go p.d.Run(ctx, updates)
	// 更新发现的服务 // 这里updates 是去读到这个数据
	go m.updater(ctx, p, updates)
}

func (m *Manager) updater(ctx context.Context, p *provider, updates chan []*targetgroup.Group) {
	for {
		select {
		case <-ctx.Done():
			return
			// 接受updates数据
		case tgs, ok := <-updates:
			receivedUpdates.WithLabelValues(m.name).Inc()
			if !ok {
				level.Debug(m.logger).Log("msg", "Discoverer channel closed", "provider", p.name)
				return
			}
			// 更新targets
			for _, s := range p.subs {
				m.updateGroup(poolKey{setName: s, provider: p.name}, tgs)
			}

			select {
			// 发送更新通知
			case m.triggerSend <- struct{}{}:
			default:
			}
		}
	}
}

// 这段代码 让我对channel通信有了新的认识
// 1。 for循环里面套用这么多select
// 2。 triggerSend 明明是等待别人发数据来，为啥，还往里面丢一个数据？
//
//	其实是为了这次处理，会一直循环等待处理。等待syncCh拿到数据
func (m *Manager) sender() {
	ticker := time.NewTicker(m.updatert)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C: // Some discoverers send updates too often so we throttle these with the ticker.
			select {
			case <-m.triggerSend:
				sentUpdates.WithLabelValues(m.name).Inc()
				select {
				case m.syncCh <- m.allGroups():
				default:
					delayedUpdates.WithLabelValues(m.name).Inc()
					level.Debug(m.logger).Log("msg", "Discovery receiver's channel was full so will retry the next cycle")
					select {
					case m.triggerSend <- struct{}{}:
					default:
					}
				}
			default:
			}
		}
	}
}

func (m *Manager) cancelDiscoverers() {
	for _, c := range m.discoverCancel {
		c()
	}
}

func (m *Manager) updateGroup(poolKey poolKey, tgs []*targetgroup.Group) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if _, ok := m.targets[poolKey]; !ok {
		m.targets[poolKey] = make(map[string]*targetgroup.Group)
	}
	for _, tg := range tgs {
		if tg != nil { // Some Discoverers send nil target group so need to check for it to avoid panics.
			m.targets[poolKey][tg.Source] = tg
		}
	}
}

func (m *Manager) allGroups() map[string][]*targetgroup.Group {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	tSets := map[string][]*targetgroup.Group{}
	n := map[string]int{}
	for pkey, tsets := range m.targets {
		for _, tg := range tsets {
			// Even if the target group 'tg' is empty we still need to send it to the 'Scrape manager'
			// to signal that it needs to stop all scrape loops for this target set.
			tSets[pkey.setName] = append(tSets[pkey.setName], tg)
			n[pkey.setName] += len(tg.Targets)
		}
	}
	for setName, v := range n {
		discoveredTargets.WithLabelValues(m.name, setName).Set(float64(v))
	}
	return tSets
}

// registerProviders returns a number of failed SD config.
func (m *Manager) registerProviders(cfgs discovery.Configs, setName string) int {
	// 标签
	var (
		failed int
		added  bool
	)
	// 加载Providers的add方法
	add := func(cfg discovery.Config) {
		// 读取cfg类型
		for _, p := range m.providers {
			// 检查该cfg是否加载过
			if reflect.DeepEqual(cfg, p.config) {
				// 如果加载过，记录该Job
				p.subs = append(p.subs, setName)
				// 变更标签状态
				added = true
				// 跳出
				return
			}
		}
		typ := cfg.Name()
		// 创建一个Discoverer实例
		d, err := cfg.NewDiscoverer(discovery.DiscovererOptions{
			Logger: log.With(m.logger, "discovery", typ, "config", setName),
		})
		if err != nil {
			level.Error(m.logger).Log("msg", "Cannot create service discovery", "err", err, "type", typ, "config", setName)
			failed++
			return
		}
		// 添加该provider到m.provider队列中
		m.providers = append(m.providers, &provider{
			// 生成provider名称
			name: fmt.Sprintf("%s/%d", typ, len(m.providers)),
			// 关联对应的Discoverer实例， （比如DNS，zk，K8等）
			d: d,
			// 关联配置
			config: cfg,
			// 关联job
			subs: []string{setName},
		})
		// 更新标签
		added = true
	}
	for _, cfg := range cfgs {
		add(cfg)
	}
	if !added {
		// Add an empty target group to force the refresh of the corresponding
		// scrape pool and to notify the receiver that this target set has no
		// current targets.
		// It can happen because the combined set of SD configurations is empty
		// or because we fail to instantiate all the SD configurations.
		add(discovery.StaticConfig{{}})
	}
	return failed
}

// StaticProvider holds a list of target groups that never change.
type StaticProvider struct {
	TargetGroups []*targetgroup.Group
}

// Run implements the Worker interface.
func (sd *StaticProvider) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	// We still have to consider that the consumer exits right away in which case
	// the context will be canceled.
	select {
	case ch <- sd.TargetGroups:
	case <-ctx.Done():
	}
	close(ch)
}
