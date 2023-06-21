// Copyright 2015 The Prometheus Authors
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

// The main package for the Prometheus server executable.
// nolint:revive // Many unsued function arguments in this file by design.
package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"net"
	"net/http"
	_ "net/http/pprof" // Comment this line to disable pprof endpoint.
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/alecthomas/units"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/regexp"
	"github.com/mwitkow/go-conntrack"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promlog"
	promlogflag "github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/common/version"
	toolkit_web "github.com/prometheus/exporter-toolkit/web"
	"go.uber.org/atomic"
	"go.uber.org/automaxprocs/maxprocs"
	"k8s.io/klog"
	klogv2 "k8s.io/klog/v2"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/legacymanager"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/notifier"
	_ "github.com/prometheus/prometheus/plugins" // Register plugins.
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tracing"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/agent"
	"github.com/prometheus/prometheus/util/documentcli"
	"github.com/prometheus/prometheus/util/logging"
	prom_runtime "github.com/prometheus/prometheus/util/runtime"
	"github.com/prometheus/prometheus/web"
)

var (
	appName = "prometheus"

	configSuccess = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_config_last_reload_successful",
		Help: "Whether the last configuration reload attempt was successful.",
	})
	configSuccessTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_config_last_reload_success_timestamp_seconds",
		Help: "Timestamp of the last successful configuration reload.",
	})

	defaultRetentionString   = "15d"
	defaultRetentionDuration model.Duration

	agentMode                       bool
	agentOnlyFlags, serverOnlyFlags []string
)

func init() {
	prometheus.MustRegister(version.NewCollector(strings.ReplaceAll(appName, "-", "_")))

	var err error
	defaultRetentionDuration, err = model.ParseDuration(defaultRetentionString)
	if err != nil {
		panic(err)
	}
}

// serverOnlyFlag creates server-only kingpin flag.
func serverOnlyFlag(app *kingpin.Application, name, help string) *kingpin.FlagClause {
	return app.Flag(name, fmt.Sprintf("%s Use with server mode only.", help)).
		PreAction(func(parseContext *kingpin.ParseContext) error {
			// This will be invoked only if flag is actually provided by user.
			serverOnlyFlags = append(serverOnlyFlags, "--"+name)
			return nil
		})
}

// agentOnlyFlag creates agent-only kingpin flag.
func agentOnlyFlag(app *kingpin.Application, name, help string) *kingpin.FlagClause {
	return app.Flag(name, fmt.Sprintf("%s Use with agent mode only.", help)).
		PreAction(func(parseContext *kingpin.ParseContext) error {
			// This will be invoked only if flag is actually provided by user.
			agentOnlyFlags = append(agentOnlyFlags, "--"+name)
			return nil
		})
}

type flagConfig struct {
	configFile string

	agentStoragePath    string
	serverStoragePath   string
	notifier            notifier.Options
	forGracePeriod      model.Duration
	outageTolerance     model.Duration
	resendDelay         model.Duration
	web                 web.Options
	scrape              scrape.Options
	tsdb                tsdbOptions
	agent               agentOptions
	lookbackDelta       model.Duration
	webTimeout          model.Duration
	queryTimeout        model.Duration
	queryConcurrency    int
	queryMaxSamples     int
	RemoteFlushDeadline model.Duration

	featureList []string
	// These options are extracted from featureList
	// for ease of use.
	enableExpandExternalLabels bool
	enableNewSDManager         bool
	enablePerStepStats         bool
	enableAutoGOMAXPROCS       bool

	prometheusURL   string
	corsRegexString string

	promlogConfig promlog.Config
}

// setFeatureListOptions sets the corresponding options from the featureList.
func (c *flagConfig) setFeatureListOptions(logger log.Logger) error {
	for _, f := range c.featureList {
		opts := strings.Split(f, ",")
		for _, o := range opts {
			switch o {
			case "remote-write-receiver":
				c.web.EnableRemoteWriteReceiver = true
				level.Warn(logger).Log("msg", "Remote write receiver enabled via feature flag remote-write-receiver. This is DEPRECATED. Use --web.enable-remote-write-receiver.")
			case "expand-external-labels":
				c.enableExpandExternalLabels = true
				level.Info(logger).Log("msg", "Experimental expand-external-labels enabled")
			case "exemplar-storage":
				c.tsdb.EnableExemplarStorage = true
				level.Info(logger).Log("msg", "Experimental in-memory exemplar storage enabled")
			case "memory-snapshot-on-shutdown":
				c.tsdb.EnableMemorySnapshotOnShutdown = true
				level.Info(logger).Log("msg", "Experimental memory snapshot on shutdown enabled")
			case "extra-scrape-metrics":
				c.scrape.ExtraMetrics = true
				level.Info(logger).Log("msg", "Experimental additional scrape metrics enabled")
			case "new-service-discovery-manager":
				c.enableNewSDManager = true
				level.Info(logger).Log("msg", "Experimental service discovery manager")
			case "agent":
				agentMode = true
				level.Info(logger).Log("msg", "Experimental agent mode enabled.")
			case "promql-per-step-stats":
				c.enablePerStepStats = true
				level.Info(logger).Log("msg", "Experimental per-step statistics reporting")
			case "auto-gomaxprocs":
				c.enableAutoGOMAXPROCS = true
				level.Info(logger).Log("msg", "Automatically set GOMAXPROCS to match Linux container CPU quota")
			case "no-default-scrape-port":
				c.scrape.NoDefaultPort = true
				level.Info(logger).Log("msg", "No default port will be appended to scrape targets' addresses.")
			case "native-histograms":
				c.tsdb.EnableNativeHistograms = true
				c.scrape.EnableProtobufNegotiation = true
				level.Info(logger).Log("msg", "Experimental native histogram support enabled.")
			case "":
				continue
			case "promql-at-modifier", "promql-negative-offset":
				level.Warn(logger).Log("msg", "This option for --enable-feature is now permanently enabled and therefore a no-op.", "option", o)
			default:
				level.Warn(logger).Log("msg", "Unknown option for --enable-feature", "option", o)
			}
		}
	}

	if c.tsdb.EnableNativeHistograms && c.tsdb.EnableMemorySnapshotOnShutdown {
		c.tsdb.EnableMemorySnapshotOnShutdown = false
		level.Warn(logger).Log("msg", "memory-snapshot-on-shutdown has been disabled automatically because memory-snapshot-on-shutdown and native-histograms cannot be enabled at the same time.")
	}

	return nil
}

func main() {
	//检查环境变量 "DEBUG" 是否存在，如果存在则设置 Go 项目的 BlockProfileRate 和 MutexProfileFraction 两个参数，
	//以提高代码的性能和 profile 能力。
	if os.Getenv("DEBUG") != "" {
		runtime.SetBlockProfileRate(20)
		runtime.SetMutexProfileFraction(20)
	}

	var (
		// 定义两个变量 oldFlagRetentionDuration 和 newFlagRetentionDuration，
		// 分别表示之前定义的用于保存指标数据的过期时间 (oldFlagRetentionDuration) 和
		// 新定义的过期时间 (newFlagRetentionDuration)。
		oldFlagRetentionDuration model.Duration
		newFlagRetentionDuration model.Duration
	)
	// 创建一个名为 cfg 的 flagConfig 对象，其中包含用于监控的 notifier、web 和 promlog 选项。
	cfg := flagConfig{
		notifier: notifier.Options{
			Registerer: prometheus.DefaultRegisterer,
		},
		web: web.Options{
			Registerer: prometheus.DefaultRegisterer,
			Gatherer:   prometheus.DefaultGatherer,
		},
		promlogConfig: promlog.Config{},
	}
	// 创建一个名为 a 的 Kingpin 命令对象，并使用 filepath.Base(os.Args[0]) 获取命令行程序的绝对路径，
	// 以及 "The Prometheus monitoring server" 作为命令名称。
	a := kingpin.New(filepath.Base(os.Args[0]), "The Prometheus monitoring server").UsageWriter(os.Stdout)

	// 使用 a.Version 方法打印出程序的版本信息，
	// 以及使用 a.HelpFlag.Short('h') 方法打印出程序的帮助信息。
	a.Version(version.Print(appName))
	a.HelpFlag.Short('h')

	/*
		使用 a.Flag 方法定义并设置 Prometheus 配置文件路径 (cfg.configFile)
		和 Web 监听地址 (cfg.web.listenAddress)。
	*/
	a.Flag("config.file", "Prometheus configuration file path.").
		Default("prometheus.yml").StringVar(&cfg.configFile)

	a.Flag("web.listen-address", "Address to listen on for UI, API, and telemetry.").
		Default("0.0.0.0:9090").StringVar(&cfg.web.ListenAddress)

	/*
		--config.file：指定 Prometheus 配置文件的路径。默认值为 prometheus.yml。
		--web.listen-address：指定 UI、API 和遥测监听的地址。默认值为 0.0.0.0:9090。
		--web.config.file：指定可以启用 TLS 或身份验证的配置文件的路径。这是一个实验性功能。默认值为空。
		--web.read-timeout：指定读取请求的超时时间，并关闭空闲连接。默认值为 5m。
		--web.max-connections：指定最大同时连接数。默认值为 512。
		--web.external-url：指定 Prometheus 外部可访问的 URL（例如，如果 Prometheus 是通过反向代理提供服务的）。用于生成相对和绝对链接，指向 Prometheus 本身。如果 URL 有路径部分，它将用于前缀所有由 Prometheus 提供服务的 HTTP 端点。如果省略，则相关的 URL 组件将自动派生。
		--web.route-prefix：指定 Web 端点内部路由的前缀。默认为 --web.external-url 的路径。
		--web.user-assets：指定静态资产目录的路径，可在 /user 下访问。默认值为空。
		--web.enable-lifecycle：启用通过 HTTP 请求进行关闭和重新加载。默认值为 false。
		--web.enable-admin-api：启用管理员控制操作的 API 端点。默认值为 false。
		--web.enable-remote-write-receiver：启用接受远程写入请求的 API 端点。默认值为 false。
		--web.console.templates：指定控制台模板目录的路径，可在 /consoles 下访问。默认值为 consoles。
		--web.console.libraries：指定控制台库目录的路径。默认值为 console_libraries。
		--web.page-title：指定 Prometheus 实例的文档标题。默认值为 “Prometheus Time Series Collection and Processing Server”。
		--web.cors.origin：CORS 源的正则表达式。它是完全锚定的。例如：‘https?://(domain1|domain2).com’。默认值为 .*。
		这些命令行参数可以在启动 Prometheus 服务器时指定，以覆盖默认配置。
	*/
	webConfig := a.Flag(
		"web.config.file",
		"[EXPERIMENTAL] Path to configuration file that can enable TLS or authentication.",
	).Default("").String()

	a.Flag("web.read-timeout",
		"Maximum duration before timing out read of the request, and closing idle connections.").
		Default("5m").SetValue(&cfg.webTimeout)

	a.Flag("web.max-connections", "Maximum number of simultaneous connections.").
		Default("512").IntVar(&cfg.web.MaxConnections)

	a.Flag("web.external-url",
		"The URL under which Prometheus is externally reachable (for example, if Prometheus is served via a reverse proxy). Used for generating relative and absolute links back to Prometheus itself. If the URL has a path portion, it will be used to prefix all HTTP endpoints served by Prometheus. If omitted, relevant URL components will be derived automatically.").
		PlaceHolder("<URL>").StringVar(&cfg.prometheusURL)

	a.Flag("web.route-prefix",
		"Prefix for the internal routes of web endpoints. Defaults to path of --web.external-url.").
		PlaceHolder("<path>").StringVar(&cfg.web.RoutePrefix)

	a.Flag("web.user-assets", "Path to static asset directory, available at /user.").
		PlaceHolder("<path>").StringVar(&cfg.web.UserAssetsPath)

	a.Flag("web.enable-lifecycle", "Enable shutdown and reload via HTTP request.").
		Default("false").BoolVar(&cfg.web.EnableLifecycle)

	a.Flag("web.enable-admin-api", "Enable API endpoints for admin control actions.").
		Default("false").BoolVar(&cfg.web.EnableAdminAPI)

	a.Flag("web.enable-remote-write-receiver", "Enable API endpoint accepting remote write requests.").
		Default("false").BoolVar(&cfg.web.EnableRemoteWriteReceiver)

	a.Flag("web.console.templates", "Path to the console template directory, available at /consoles.").
		Default("consoles").StringVar(&cfg.web.ConsoleTemplatesPath)

	a.Flag("web.console.libraries", "Path to the console library directory.").
		Default("console_libraries").StringVar(&cfg.web.ConsoleLibrariesPath)

	a.Flag("web.page-title", "Document title of Prometheus instance.").
		Default("Prometheus Time Series Collection and Processing Server").StringVar(&cfg.web.PageTitle)

	a.Flag("web.cors.origin", `Regex for CORS origin. It is fully anchored. Example: 'https?://(domain1|domain2)\.com'`).
		Default(".*").StringVar(&cfg.corsRegexString)

	/*
		这段代码定义了用于配置 Prometheus 存储桶 (storage) 的参数。这些参数用于指定 Prometheus 存储桶中数据存储的基路径、数据块最小持久化时间、数据块最大持久化时间、数据块最大片段大小、wal 片段大小以及数据保留时间。
		具体来说，这些参数包括:
		"storage.tsdb.path" 指定了用于存储 Prometheus 数据基路径。该路径将以 data/ 开头，用于存储 Prometheus 存储桶中的数据。
		"storage.tsdb.min-block-duration" 指定了数据块最小持久化时间，该时间将以秒为单位进行设置。此参数仅在测试环境中使用。
		"storage.tsdb.max-block-duration" 指定了数据块最大持久化时间，该时间将以秒为单位进行设置。此参数仅在测试环境中使用。默认情况下，此参数的值为 10%。
		"storage.tsdb.max-block-chunk-segment-size" 指定了数据块中单个片段的最大大小，单位为兆字节。
		"storage.tsdb.wal-segment-size" 指定了 tsdb wal 片段文件的最大大小，单位为兆字节。
		"storage.tsdb.retention" 指定了数据保留时间，该时间将以秒为单位进行设置。此参数已过期，应使用 "storage.tsdb.retention.time" 替代。
		"storage.tsdb.retention.time" 指定了数据保留时间，该时间将以秒为单位进行设置。当此参数被设置时，它将覆盖 "storage.tsdb.retention" 参数。如果未设置此参数、"storage.tsdb.retention" 或 "storage.tsdb.retention.size",则默认保留时间为 14 天。支持的单位有年、周、天、小时、分钟、秒和毫秒。
		"storage.tsdb.no-lockfile" 指定了是否在数据目录中创建锁文件。默认为 false。
		这些参数可以被用于配置 Prometheus 存储桶的参数，以允许在存储桶中存储更多数据，并提高数据持久性。
	*/
	serverOnlyFlag(a, "storage.tsdb.path", "Base path for metrics storage.").
		Default("data/").StringVar(&cfg.serverStoragePath)

	serverOnlyFlag(a, "storage.tsdb.min-block-duration", "Minimum duration of a data block before being persisted. For use in testing.").
		Hidden().Default("2h").SetValue(&cfg.tsdb.MinBlockDuration)

	serverOnlyFlag(a, "storage.tsdb.max-block-duration",
		"Maximum duration compacted blocks may span. For use in testing. (Defaults to 10% of the retention period.)").
		Hidden().PlaceHolder("<duration>").SetValue(&cfg.tsdb.MaxBlockDuration)

	serverOnlyFlag(a, "storage.tsdb.max-block-chunk-segment-size",
		"The maximum size for a single chunk segment in a block. Example: 512MB").
		Hidden().PlaceHolder("<bytes>").BytesVar(&cfg.tsdb.MaxBlockChunkSegmentSize)

	serverOnlyFlag(a, "storage.tsdb.wal-segment-size",
		"Size at which to split the tsdb WAL segment files. Example: 100MB").
		Hidden().PlaceHolder("<bytes>").BytesVar(&cfg.tsdb.WALSegmentSize)

	serverOnlyFlag(a, "storage.tsdb.retention", "[DEPRECATED] How long to retain samples in storage. This flag has been deprecated, use \"storage.tsdb.retention.time\" instead.").
		SetValue(&oldFlagRetentionDuration)

	serverOnlyFlag(a, "storage.tsdb.retention.time", "How long to retain samples in storage. When this flag is set it overrides \"storage.tsdb.retention\". If neither this flag nor \"storage.tsdb.retention\" nor \"storage.tsdb.retention.size\" is set, the retention time defaults to "+defaultRetentionString+". Units Supported: y, w, d, h, m, s, ms.").
		SetValue(&newFlagRetentionDuration)

	serverOnlyFlag(a, "storage.tsdb.retention.size", "Maximum number of bytes that can be stored for blocks. A unit is required, supported units: B, KB, MB, GB, TB, PB, EB. Ex: \"512MB\". Based on powers-of-2, so 1KB is 1024B.").
		BytesVar(&cfg.tsdb.MaxBytes)

	serverOnlyFlag(a, "storage.tsdb.no-lockfile", "Do not create lockfile in data directory.").
		Default("false").BoolVar(&cfg.tsdb.NoLockfile)

	// TODO: Remove in Prometheus 3.0.
	/*
		这段代码定义了一些命令行标志，用于配置存储选项。它使用了两个函数：serverOnlyFlag 和 agentOnlyFlag，这两个函数分别用于定义仅服务器和仅代理的标志。
		storage.tsdb.allow-overlapping-blocks 的标志，它已被弃用，现在默认启用重叠块。该标志的默认值为 true，并且被隐藏。它的值存储在变量 b 中。
		storage.tsdb.wal-compression，它用于压缩 tsdb WAL。它的默认值为 true，并且被隐藏。它的值存储在变量 cfg.tsdb.WALCompression 中。
		storage.tsdb.head-chunks-write-queue-size：用于定义通过 head chunks 写入磁盘以进行 m-mapping 的队列的大小。默认值为 0，表示完全禁用队列。此标志为实验性质。它的值存储在变量 cfg.tsdb.HeadChunksWriteQueueSize 中。
		storage.tsdb.samples-per-chunk：用于定义每个块的目标样本数。默认值为 120，并且被隐藏。它的值存储在变量 cfg.tsdb.SamplesPerChunk 中。
		storage.agent.path：用于定义度量存储的基本路径。默认值为 data-agent/。它的值存储在变量 cfg.agentStoragePath 中。
		storage.agent.wal-segment-size：用于定义拆分 WAL 段文件的大小。例如：100MB。它被隐藏，并使用占位符 <bytes>。它的值存储在变量 cfg.agent.WALSegmentSize 中。
		storage.agent.wal-compression：用于压缩代理 WAL。默认值为 true。它的值存储在变量 cfg.agent.WALCompression 中。
		storage.agent.wal-truncate-frequency：用于定义截断 WAL 并删除旧数据的频率。它被隐藏，并使用占位符 <duration>。它的值存储在变量 cfg.agent.TruncateFrequency 中。
		storage.agent.retention.min-time：用于定义样本在被考虑删除时的最小年龄（当 WAL 被截断时）。它的值存储在变量 cfg.agent.MinWALTime 中。
		storage.agent.retention.max-time：用于定义样本在被强制删除时的最大年龄（当 WAL 被截断时）。它的值存储在变量 cfg.agent.MaxWALTime 中。
		storage.agent.no-lockfile：用于指定是否在数据目录中创建锁文件。默认值为 false。它的值存储在变量 cfg.agent.NoLockfile 中。
	*/
	var b bool
	serverOnlyFlag(a, "storage.tsdb.allow-overlapping-blocks", "[DEPRECATED] This flag has no effect. Overlapping blocks are enabled by default now.").
		Default("true").Hidden().BoolVar(&b)

	serverOnlyFlag(a, "storage.tsdb.wal-compression", "Compress the tsdb WAL.").
		Hidden().Default("true").BoolVar(&cfg.tsdb.WALCompression)

	serverOnlyFlag(a, "storage.tsdb.head-chunks-write-queue-size", "Size of the queue through which head chunks are written to the disk to be m-mapped, 0 disables the queue completely. Experimental.").
		Default("0").IntVar(&cfg.tsdb.HeadChunksWriteQueueSize)

	serverOnlyFlag(a, "storage.tsdb.samples-per-chunk", "Target number of samples per chunk.").
		Default("120").Hidden().IntVar(&cfg.tsdb.SamplesPerChunk)

	agentOnlyFlag(a, "storage.agent.path", "Base path for metrics storage.").
		Default("data-agent/").StringVar(&cfg.agentStoragePath)

	agentOnlyFlag(a, "storage.agent.wal-segment-size",
		"Size at which to split WAL segment files. Example: 100MB").
		Hidden().PlaceHolder("<bytes>").BytesVar(&cfg.agent.WALSegmentSize)

	agentOnlyFlag(a, "storage.agent.wal-compression", "Compress the agent WAL.").
		Default("true").BoolVar(&cfg.agent.WALCompression)

	agentOnlyFlag(a, "storage.agent.wal-truncate-frequency",
		"The frequency at which to truncate the WAL and remove old data.").
		Hidden().PlaceHolder("<duration>").SetValue(&cfg.agent.TruncateFrequency)

	agentOnlyFlag(a, "storage.agent.retention.min-time",
		"Minimum age samples may be before being considered for deletion when the WAL is truncated").
		SetValue(&cfg.agent.MinWALTime)

	agentOnlyFlag(a, "storage.agent.retention.max-time",
		"Maximum age samples may be before being forcibly deleted when the WAL is truncated").
		SetValue(&cfg.agent.MaxWALTime)

	agentOnlyFlag(a, "storage.agent.no-lockfile", "Do not create lockfile in data directory.").
		Default("false").BoolVar(&cfg.agent.NoLockfile)

	/*
		storage.remote.flush-deadline 的标志，用于指定在关闭或重新加载配置时刷新样本的等待时间。默认值为 1m，并使用占位符 <duration>。它的值存储在变量 cfg.RemoteFlushDeadline 中。
		storage.remote.read-sample-limit，它用于定义通过远程读取接口返回的单个查询中的最大样本总数。默认值为 5e7，0 表示无限制。对于流式响应类型，此限制将被忽略。它的值存储在变量 cfg.web.RemoteReadSampleLimit 中。
		storage.remote.read-concurrent-limit：用于定义最大并发远程读取调用数。默认值为 10，0 表示无限制。它的值存储在变量 cfg.web.RemoteReadConcurrencyLimit 中。
		storage.remote.read-max-bytes-in-frame：用于定义在编组之前流式远程读取响应类型的单个帧中的最大字节数。请注意，客户端也可能对帧大小有限制。默认值为 1048576（1MB），这是 protobuf 推荐的默认值。它的值存储在变量 cfg.web.RemoteReadBytesInFrame 中。
		rules.alert.for-outage-tolerance：用于定义在恢复警报的 “for” 状态时可以容忍 prometheus 中断的最长时间。默认值为 1h。它的值存储在变量 cfg.outageTolerance 中。
		rules.alert.for-grace-period：用于定义警报和恢复 “for” 状态之间的最短持续时间。仅对配置了大于宽限期的 “for” 时间的警报进行维护。默认值为 10m。它的值存储在变量 cfg.forGracePeriod 中。
		rules.alert.resend-delay：用于定义在向 Alertmanager 重新发送警报之前等待的最短时间。默认值为 1m。它的值存储在变量 cfg.resendDelay 中。
		scrape.adjust-timestamps：用于调整抓取时间戳，以便将它们与预期的时间表对齐，最多可调整 scrape.timestamp-tolerance。有关更多上下文，请参见 https://github.com/prometheus/prometheus/issues/7846 。这是实验性质的。此标志将在未来版本中删除。它被隐藏，并且默认值为 true。它的值存储在变量 scrape.AlignScrapeTimestamps 中。
		scrape.timestamp-tolerance：时间戳容差。有关更多上下文，请参见 https://github.com/prometheus/prometheus/issues/7846 。这是实验性质的。此标志将在未来版本中删除。它被隐藏，并且默认值为 2ms。它的值存储在变量 scrape.ScrapeTimestampTolerance 中。
		alertmanager.notification-queue-capacity：用于定义待处理 Alertmanager 通知队列的容量。默认值为 10000。它的值存储在变量 cfg.notifier.QueueCapacity 中。
	*/
	a.Flag("storage.remote.flush-deadline", "How long to wait flushing sample on shutdown or config reload.").
		Default("1m").PlaceHolder("<duration>").SetValue(&cfg.RemoteFlushDeadline)

	serverOnlyFlag(a, "storage.remote.read-sample-limit", "Maximum overall number of samples to return via the remote read interface, in a single query. 0 means no limit. This limit is ignored for streamed response types.").
		Default("5e7").IntVar(&cfg.web.RemoteReadSampleLimit)

	serverOnlyFlag(a, "storage.remote.read-concurrent-limit", "Maximum number of concurrent remote read calls. 0 means no limit.").
		Default("10").IntVar(&cfg.web.RemoteReadConcurrencyLimit)

	serverOnlyFlag(a, "storage.remote.read-max-bytes-in-frame", "Maximum number of bytes in a single frame for streaming remote read response types before marshalling. Note that client might have limit on frame size as well. 1MB as recommended by protobuf by default.").
		Default("1048576").IntVar(&cfg.web.RemoteReadBytesInFrame)

	serverOnlyFlag(a, "rules.alert.for-outage-tolerance", "Max time to tolerate prometheus outage for restoring \"for\" state of alert.").
		Default("1h").SetValue(&cfg.outageTolerance)

	serverOnlyFlag(a, "rules.alert.for-grace-period", "Minimum duration between alert and restored \"for\" state. This is maintained only for alerts with configured \"for\" time greater than grace period.").
		Default("10m").SetValue(&cfg.forGracePeriod)

	serverOnlyFlag(a, "rules.alert.resend-delay", "Minimum amount of time to wait before resending an alert to Alertmanager.").
		Default("1m").SetValue(&cfg.resendDelay)

	a.Flag("scrape.adjust-timestamps", "Adjust scrape timestamps by up to `scrape.timestamp-tolerance` to align them to the intended schedule. See https://github.com/prometheus/prometheus/issues/7846 for more context. Experimental. This flag will be removed in a future release.").
		Hidden().Default("true").BoolVar(&scrape.AlignScrapeTimestamps)

	a.Flag("scrape.timestamp-tolerance", "Timestamp tolerance. See https://github.com/prometheus/prometheus/issues/7846 for more context. Experimental. This flag will be removed in a future release.").
		Hidden().Default("2ms").DurationVar(&scrape.ScrapeTimestampTolerance)

	serverOnlyFlag(a, "alertmanager.notification-queue-capacity", "The capacity of the queue for pending Alertmanager notifications.").
		Default("10000").IntVar(&cfg.notifier.QueueCapacity)

	// TODO: Remove in Prometheus 3.0.
	alertmanagerTimeout := a.Flag("alertmanager.timeout", "[DEPRECATED] This flag has no effect.").Hidden().String()

	serverOnlyFlag(a, "query.lookback-delta", "The maximum lookback duration for retrieving metrics during expression evaluations and federation.").
		Default("5m").SetValue(&cfg.lookbackDelta)

	serverOnlyFlag(a, "query.timeout", "Maximum time a query may take before being aborted.").
		Default("2m").SetValue(&cfg.queryTimeout)

	serverOnlyFlag(a, "query.max-concurrency", "Maximum number of queries executed concurrently.").
		Default("20").IntVar(&cfg.queryConcurrency)

	serverOnlyFlag(a, "query.max-samples", "Maximum number of samples a single query can load into memory. Note that queries will fail if they try to load more samples than this into memory, so this also limits the number of samples a query can return.").
		Default("50000000").IntVar(&cfg.queryMaxSamples)

	a.Flag("scrape.discovery-reload-interval", "Interval used by scrape manager to throttle target groups updates.").
		Hidden().Default("5s").SetValue(&cfg.scrape.DiscoveryReloadInterval)

	a.Flag("enable-feature", "Comma separated feature names to enable. Valid options: agent, exemplar-storage, expand-external-labels, memory-snapshot-on-shutdown, promql-at-modifier, promql-negative-offset, promql-per-step-stats, remote-write-receiver (DEPRECATED), extra-scrape-metrics, new-service-discovery-manager, auto-gomaxprocs, no-default-scrape-port, native-histograms. See https://prometheus.io/docs/prometheus/latest/feature_flags/ for more details.").
		Default("").StringsVar(&cfg.featureList)
	/*
		使用 promlogflag.AddFlags 函数向命令行应用程序添加了日志记录标志。它使用了变量 cfg.promlogConfig 来配置日志记录选项。
		接下来，定义了一个名为 write-documentation 的标志，用于生成命令行文档。此标志仅供内部使用，并且被隐藏。当此标志被指定时，它将调用 documentcli.GenerateMarkdown 函数来生成文档，并将其输出到标准输出。如果发生错误，它将退出程序并返回错误。
		然后，代码解析命令行参数。如果发生错误，它将在标准错误上打印错误消息，并显示用法信息，然后退出程序。
		接下来，代码使用 promlog.New 函数创建了一个新的日志记录器。它使用了变量 cfg.promlogConfig 来配置日志记录选项。
		然后，代码调用 cfg.setFeatureListOptions 函数来设置功能列表选项。如果发生错误，它将在标准错误上打印错误消息并退出程序。
		接下来，代码检查是否在代理模式下运行，并且是否指定了仅服务器标志。如果是，则在标准错误上打印错误消息并退出程序。
		然后，代码检查是否在非代理模式下运行，并且是否指定了仅代理标志。如果是，则在标准错误上打印错误消息并退出程序。
		最后，代码根据当前模式设置本地存储路径。如果处于代理模式，则使用变量 cfg.agentStoragePath；否则，使用变量 cfg.serverStoragePath。
	*/
	promlogflag.AddFlags(a, &cfg.promlogConfig)

	a.Flag("write-documentation", "Generate command line documentation. Internal use.").Hidden().Action(func(ctx *kingpin.ParseContext) error {
		if err := documentcli.GenerateMarkdown(a.Model(), os.Stdout); err != nil {
			os.Exit(1)
			return err
		}
		os.Exit(0)
		return nil
	}).Bool()

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("Error parsing commandline arguments: %w", err))
		a.Usage(os.Args[1:])
		os.Exit(2)
	}

	logger := promlog.New(&cfg.promlogConfig)

	if err := cfg.setFeatureListOptions(logger); err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("Error parsing feature list: %w", err))
		os.Exit(1)
	}

	if agentMode && len(serverOnlyFlags) > 0 {
		fmt.Fprintf(os.Stderr, "The following flag(s) can not be used in agent mode: %q", serverOnlyFlags)
		os.Exit(3)
	}

	if !agentMode && len(agentOnlyFlags) > 0 {
		fmt.Fprintf(os.Stderr, "The following flag(s) can only be used in agent mode: %q", agentOnlyFlags)
		os.Exit(3)
	}

	localStoragePath := cfg.serverStoragePath
	if agentMode {
		localStoragePath = cfg.agentStoragePath
	}
	/*
		computeExternalURL 函数来计算外部 URL。它使用了变量 cfg.prometheusURL 和 cfg.web.ListenAddress 来计算 URL。如果发生错误，它将在标准错误上打印错误消息并退出程序。
			computeExternalURL 函数用于计算 Prometheus 服务器的外部 URL。它接受两个参数：prometheusURL 和 listenAddress。
			prometheusURL 参数是一个字符串，表示 Prometheus 服务器的外部 URL。如果此参数为空，则将使用 listenAddress 参数来计算外部 URL。
			listenAddress 参数是一个字符串，表示 Prometheus 服务器的监听地址。它用于计算外部 URL，当 prometheusURL 参数为空时。
		接下来，代码调用 compileCORSRegexString 函数来编译 CORS 正则表达式。它使用了变量 cfg.corsRegexString 来编译正则表达式。如果发生错误，它将在标准错误上打印错误消息并退出程序。
		最后，代码检查是否指定了 alertmanager.timeout 标志。如果是，则在日志记录器上打印警告消息，指出该标志没有效果，并将在未来删除。
	*/
	cfg.web.ExternalURL, err = computeExternalURL(cfg.prometheusURL, cfg.web.ListenAddress)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("parse external URL %q: %w", cfg.prometheusURL, err))
		os.Exit(2)
	}

	cfg.web.CORSOrigin, err = compileCORSRegexString(cfg.corsRegexString)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("could not compile CORS regex string %q: %w", cfg.corsRegexString, err))
		os.Exit(2)
	}

	if *alertmanagerTimeout != "" {
		level.Warn(logger).Log("msg", "The flag --alertmanager.timeout has no effect and will be removed in the future.")
	}

	// Throw error for invalid config before starting other components.
	/*
		代码使用 config.LoadFile 函数加载配置文件。它使用了变量 cfg.configFile 来指定配置文件的路径，以及变量 agentMode 来指定是否处于代理模式。如果发生错误，它将在日志记录器上打印错误消息并退出程序。
		接下来，代码使用 cfgFile.GetScrapeConfigs 函数检查抓取配置是否有效。如果发生错误，它将在日志记录器上打印错误消息并退出程序。
		然后，代码检查是否启用了示例存储。如果是，则检查配置文件中是否存在 ExemplarsConfig 配置。如果不存在，则使用默认配置。然后，它将变量 cfg.tsdb.MaxExemplars 设置为配置文件中指定的最大示例数。
		最后，代码检查配置文件中是否存在 TSDBConfig 配置。如果存在，则将变量 cfg.tsdb.OutOfOrderTimeWindow 设置为配置文件中指定的值。
	*/
	var cfgFile *config.Config
	if cfgFile, err = config.LoadFile(cfg.configFile, agentMode, false, log.NewNopLogger()); err != nil {
		absPath, pathErr := filepath.Abs(cfg.configFile)
		if pathErr != nil {
			absPath = cfg.configFile
		}
		level.Error(logger).Log("msg", fmt.Sprintf("Error loading config (--config.file=%s)", cfg.configFile), "file", absPath, "err", err)
		os.Exit(2)
	}
	if _, err := cfgFile.GetScrapeConfigs(); err != nil {
		absPath, pathErr := filepath.Abs(cfg.configFile)
		if pathErr != nil {
			absPath = cfg.configFile
		}
		level.Error(logger).Log("msg", fmt.Sprintf("Error loading scrape config files from config (--config.file=%q)", cfg.configFile), "file", absPath, "err", err)
		os.Exit(2)
	}
	if cfg.tsdb.EnableExemplarStorage {
		if cfgFile.StorageConfig.ExemplarsConfig == nil {
			cfgFile.StorageConfig.ExemplarsConfig = &config.DefaultExemplarsConfig
		}
		cfg.tsdb.MaxExemplars = cfgFile.StorageConfig.ExemplarsConfig.MaxExemplars
	}
	if cfgFile.StorageConfig.TSDBConfig != nil {
		cfg.tsdb.OutOfOrderTimeWindow = cfgFile.StorageConfig.TSDBConfig.OutOfOrderTimeWindow
	}

	// Now that the validity of the config is established, set the config
	// success metrics accordingly, although the config isn't really loaded
	// yet. This will happen later (including setting these metrics again),
	// but if we don't do it now, the metrics will stay at zero until the
	// startup procedure is complete, which might take long enough to
	// trigger alerts about an invalid config.
	/*
		configSuccess 和 configSuccessTime 变量分别用于设置配置成功标志和配置成功时间。
		cfg.web.ReadTimeout 变量用于设置 Web 读取超时。它的值从 cfg.webTimeout 变量中获取。
		接下来，代码检查是否指定了 cfg.web.RoutePrefix 变量。如果未指定，则将其设置为 cfg.web.ExternalURL.Path 变量的值。然后，它确保 cfg.web.RoutePrefix 变量的值至少为 /。
		接下来，代码检查是否处于代理模式。如果不是，则执行以下操作：
			检查是否指定了旧的保留持续时间标志。如果是，则在日志记录器上打印警告消息，并将 cfg.tsdb.RetentionDuration 变量设置为旧标志的值。
			检查是否指定了新的保留持续时间标志。如果是，则将 cfg.tsdb.RetentionDuration 变量设置为新标志的值。
			如果未指定保留持续时间或最大字节数，则将 cfg.tsdb.RetentionDuration 变量设置为默认保留持续时间，并在日志记录器上打印信息消息。
			检查保留持续时间是否溢出。如果是，则将其限制为 100 年，并在日志记录器上打印警告消息。
			检查是否指定了最大块大小。如果未指定，则计算最大块大小，并将 cfg.tsdb.MaxBlockDuration 变量设置为计算出的值。
	*/
	configSuccess.Set(1)
	configSuccessTime.SetToCurrentTime()

	cfg.web.ReadTimeout = time.Duration(cfg.webTimeout)
	// Default -web.route-prefix to path of -web.external-url.
	if cfg.web.RoutePrefix == "" {
		cfg.web.RoutePrefix = cfg.web.ExternalURL.Path
	}
	// RoutePrefix must always be at least '/'.
	cfg.web.RoutePrefix = "/" + strings.Trim(cfg.web.RoutePrefix, "/")

	if !agentMode {
		// Time retention settings.
		if oldFlagRetentionDuration != 0 {
			level.Warn(logger).Log("deprecation_notice", "'storage.tsdb.retention' flag is deprecated use 'storage.tsdb.retention.time' instead.")
			cfg.tsdb.RetentionDuration = oldFlagRetentionDuration
		}

		// When the new flag is set it takes precedence.
		if newFlagRetentionDuration != 0 {
			cfg.tsdb.RetentionDuration = newFlagRetentionDuration
		}

		if cfg.tsdb.RetentionDuration == 0 && cfg.tsdb.MaxBytes == 0 {
			cfg.tsdb.RetentionDuration = defaultRetentionDuration
			level.Info(logger).Log("msg", "No time or size retention was set so using the default time retention", "duration", defaultRetentionDuration)
		}

		// Check for overflows. This limits our max retention to 100y.
		if cfg.tsdb.RetentionDuration < 0 {
			y, err := model.ParseDuration("100y")
			if err != nil {
				panic(err)
			}
			cfg.tsdb.RetentionDuration = y
			level.Warn(logger).Log("msg", "Time retention value is too high. Limiting to: "+y.String())
		}

		// Max block size settings.
		if cfg.tsdb.MaxBlockDuration == 0 {
			maxBlockDuration, err := model.ParseDuration("31d")
			if err != nil {
				panic(err)
			}
			// When the time retention is set and not too big use to define the max block duration.
			if cfg.tsdb.RetentionDuration != 0 && cfg.tsdb.RetentionDuration/10 < maxBlockDuration {
				maxBlockDuration = cfg.tsdb.RetentionDuration / 10
			}

			cfg.tsdb.MaxBlockDuration = maxBlockDuration
		}
	}
	/*
		noStepSubqueryInterval 变量，用于存储无步长子查询间隔。它使用了 config.DefaultGlobalConfig.EvaluationInterval 变量来设置初始值。
		接下来，代码使用 klog.ClampLevel 和 klogv2.ClampLevel 函数将 k8s 客户端的日志级别限制在 6 以下。这样，k8s 客户端就不会以明文形式记录承载令牌。
		然后，代码使用 klog.SetLogger 和 klogv2.SetLogger 函数设置 k8s 客户端的日志记录器。它使用了带有 component 标签的日志记录器。
		接下来，代码根据当前模式设置 modeAppName 和 mode 变量的值。如果处于代理模式，则将它们分别设置为 Prometheus Agent 和 agent；否则，将它们分别设置为 Prometheus Server 和 server。
		然后，在日志记录器上打印启动消息，并显示当前模式和版本信息。
		接下来，代码检查当前架构是否为 64 位。如果不是，则在日志记录器上打印警告消息，建议切换到 64 位 Prometheus 二进制文件。
		然后，在日志记录器上打印构建上下文、主机详细信息、文件描述符限制和虚拟内存限制信息。
		接下来，代码创建了一些变量，包括本地存储、抓取管理器、远程存储和扇出存储。它们用于管理存储和抓取操作。
		最后，代码创建了一些上下文变量和发现管理器变量。它们用于管理 Web、规则、抓取和通知操作。
	*/
	noStepSubqueryInterval := &safePromQLNoStepSubqueryInterval{}
	noStepSubqueryInterval.Set(config.DefaultGlobalConfig.EvaluationInterval)

	// Above level 6, the k8s client would log bearer tokens in clear-text.
	klog.ClampLevel(6)
	klog.SetLogger(log.With(logger, "component", "k8s_client_runtime"))
	klogv2.ClampLevel(6)
	klogv2.SetLogger(log.With(logger, "component", "k8s_client_runtime"))

	modeAppName := "Prometheus Server"
	mode := "server"
	if agentMode {
		modeAppName = "Prometheus Agent"
		mode = "agent"
	}

	level.Info(logger).Log("msg", "Starting "+modeAppName, "mode", mode, "version", version.Info())
	if bits.UintSize < 64 {
		level.Warn(logger).Log("msg", "This Prometheus binary has not been compiled for a 64-bit architecture. Due to virtual memory constraints of 32-bit systems, it is highly recommended to switch to a 64-bit binary of Prometheus.", "GOARCH", runtime.GOARCH)
	}

	level.Info(logger).Log("build_context", version.BuildContext())
	level.Info(logger).Log("host_details", prom_runtime.Uname())
	level.Info(logger).Log("fd_limits", prom_runtime.FdLimits())
	level.Info(logger).Log("vm_limits", prom_runtime.VMLimits())

	var (
		localStorage  = &readyStorage{stats: tsdb.NewDBStats()}
		scraper       = &readyScrapeManager{}
		remoteStorage = remote.NewStorage(log.With(logger, "component", "remote"), prometheus.DefaultRegisterer, localStorage.StartTime, localStoragePath, time.Duration(cfg.RemoteFlushDeadline), scraper)
		fanoutStorage = storage.NewFanout(logger, localStorage, remoteStorage)
	)

	var (
		ctxWeb, cancelWeb = context.WithCancel(context.Background())
		ctxRule           = context.Background()

		notifierManager = notifier.NewManager(&cfg.notifier, log.With(logger, "component", "notifier"))

		ctxScrape, cancelScrape = context.WithCancel(context.Background())
		ctxNotify, cancelNotify = context.WithCancel(context.Background())
		discoveryManagerScrape  discoveryManager
		discoveryManagerNotify  discoveryManager
	)

	/*
		代码检查是否启用了新的服务发现管理器。如果启用，则使用 discovery.NewManager 函数创建新的发现管理器；否则，使用 legacymanager.NewManager 函数创建旧的发现管理器。
		然后，代码创建了一些变量，包括抓取管理器和追踪管理器。它们用于管理抓取和追踪操作。
		接下来，代码检查是否启用了自动 GOMAXPROCS。如果启用，则使用 maxprocs.Set 函数自动设置 GOMAXPROCS。如果发生错误，则在日志记录器上打印警告消息。
		然后，代码检查是否处于代理模式。如果不是，则执行以下操作：
		创建一个 opts 变量，用于存储查询引擎选项。
		使用 promql.NewEngine 函数创建一个新的查询引擎。
		使用 rules.NewManager 函数创建一个新的规则管理器。
	*/
	if cfg.enableNewSDManager {
		discovery.RegisterMetrics()
		discoveryManagerScrape = discovery.NewManager(ctxScrape, log.With(logger, "component", "discovery manager scrape"), discovery.Name("scrape"))
		discoveryManagerNotify = discovery.NewManager(ctxNotify, log.With(logger, "component", "discovery manager notify"), discovery.Name("notify"))
	} else {
		legacymanager.RegisterMetrics()
		discoveryManagerScrape = legacymanager.NewManager(ctxScrape, log.With(logger, "component", "discovery manager scrape"), legacymanager.Name("scrape"))
		discoveryManagerNotify = legacymanager.NewManager(ctxNotify, log.With(logger, "component", "discovery manager notify"), legacymanager.Name("notify"))
	}

	var (
		scrapeManager  = scrape.NewManager(&cfg.scrape, log.With(logger, "component", "scrape manager"), fanoutStorage)
		tracingManager = tracing.NewManager(logger)

		queryEngine *promql.Engine
		ruleManager *rules.Manager
	)

	if cfg.enableAutoGOMAXPROCS {
		l := func(format string, a ...interface{}) {
			level.Info(logger).Log("component", "automaxprocs", "msg", fmt.Sprintf(strings.TrimPrefix(format, "maxprocs: "), a...))
		}
		if _, err := maxprocs.Set(maxprocs.Logger(l)); err != nil {
			level.Warn(logger).Log("component", "automaxprocs", "msg", "Failed to set GOMAXPROCS automatically", "err", err)
		}
	}

	if !agentMode {
		opts := promql.EngineOpts{
			Logger:                   log.With(logger, "component", "query engine"),
			Reg:                      prometheus.DefaultRegisterer,
			MaxSamples:               cfg.queryMaxSamples,
			Timeout:                  time.Duration(cfg.queryTimeout),
			ActiveQueryTracker:       promql.NewActiveQueryTracker(localStoragePath, cfg.queryConcurrency, log.With(logger, "component", "activeQueryTracker")),
			LookbackDelta:            time.Duration(cfg.lookbackDelta),
			NoStepSubqueryIntervalFn: noStepSubqueryInterval.Get,
			// EnableAtModifier and EnableNegativeOffset have to be
			// always on for regular PromQL as of Prometheus v2.33.
			EnableAtModifier:     true,
			EnableNegativeOffset: true,
			EnablePerStepStats:   cfg.enablePerStepStats,
		}

		queryEngine = promql.NewEngine(opts)

		ruleManager = rules.NewManager(&rules.ManagerOptions{
			Appendable:      fanoutStorage,                                                   //存储器
			Queryable:       localStorage,                                                    //本地时序数据库TSDB
			QueryFunc:       rules.EngineQueryFunc(queryEngine, fanoutStorage),               //rules计算
			NotifyFunc:      rules.SendAlerts(notifierManager, cfg.web.ExternalURL.String()), //告警通知
			Context:         ctxRule,                                                         //用于控制ruleManager组件的协程
			ExternalURL:     cfg.web.ExternalURL,                                             //通过Web对外开放的URL
			Registerer:      prometheus.DefaultRegisterer,
			Logger:          log.With(logger, "component", "rule manager"),
			OutageTolerance: time.Duration(cfg.outageTolerance), //当prometheus重启时，保持alert状态（https://ganeshvernekar.com/gsoc-2018/persist-for-state/）
			ForGracePeriod:  time.Duration(cfg.forGracePeriod),
			ResendDelay:     time.Duration(cfg.resendDelay),
		})
	}

	scraper.Set(scrapeManager)

	cfg.web.Context = ctxWeb
	cfg.web.TSDBRetentionDuration = cfg.tsdb.RetentionDuration
	cfg.web.TSDBMaxBytes = cfg.tsdb.MaxBytes
	cfg.web.TSDBDir = localStoragePath
	cfg.web.LocalStorage = localStorage
	cfg.web.Storage = fanoutStorage
	cfg.web.ExemplarStorage = localStorage
	cfg.web.QueryEngine = queryEngine
	cfg.web.ScrapeManager = scrapeManager
	cfg.web.RuleManager = ruleManager
	cfg.web.Notifier = notifierManager
	cfg.web.LookbackDelta = time.Duration(cfg.lookbackDelta)
	cfg.web.IsAgent = agentMode
	cfg.web.AppName = modeAppName
	//Web组件用于为Storage组件，queryEngine组件，scrapeManager组件， ruleManager组件和notifier 组件提供外部HTTP访问方式，也就是我们经常访问的prometheus的界面。
	cfg.web.Version = &web.PrometheusVersion{
		Version:   version.Version,
		Revision:  version.Revision,
		Branch:    version.Branch,
		BuildUser: version.BuildUser,
		BuildDate: version.BuildDate,
		GoVersion: version.GoVersion,
	}

	cfg.web.Flags = map[string]string{}

	// Exclude kingpin default flags to expose only Prometheus ones.
	/*
		首先，代码创建了一个 boilerplateFlags 变量，用于存储默认标志。它使用了 kingpin.New 函数来创建一个新的命令行应用程序。
		然后，代码遍历命令行应用程序的标志。对于每个标志，它检查是否在 boilerplateFlags 变量中存在。如果存在，则跳过该标志；否则，将其添加到 cfg.web.Flags 变量中。
		接下来，代码创建了一个 webHandler 变量，用于存储 Web 处理程序。它使用了 web.New 函数来创建 Web 处理程序。
		然后，代码使用 conntrack.NewDialContextFunc 函数创建一个新的拨号上下文函数，并将其设置为默认传输的拨号上下文函数。这样，就可以使用 conntrack 来监视默认传输上的传出连接。
		最后，代码创建了一个 externalURL 变量，用于存储外部 URL。它的值从 cfg.web.ExternalURL 变量中获取。
	*/
	boilerplateFlags := kingpin.New("", "").Version("")
	for _, f := range a.Model().Flags {
		if boilerplateFlags.GetFlag(f.Name) != nil {
			continue
		}

		cfg.web.Flags[f.Name] = f.Value.String()
	}

	// Depends on cfg.web.ScrapeManager so needs to be after cfg.web.ScrapeManager = scrapeManager.
	webHandler := web.New(log.With(logger, "component", "web"), &cfg.web)

	// Monitor outgoing connections on default transport with conntrack.
	http.DefaultTransport.(*http.Transport).DialContext = conntrack.NewDialContextFunc(
		conntrack.DialWithTracing(),
	)

	// This is passed to ruleManager.Update().
	externalURL := cfg.web.ExternalURL.String()
	/*
		 reloaders 数组，用于存储重新加载器。每个重新加载器都有一个名称和一个重新加载函数。
		例如，第一个重新加载器的名称为 db_storage，它的重新加载函数为 localStorage.ApplyConfig。当重新加载配置时，此函数将被调用以应用新的配置。
		其他重新加载器的名称和重新加载函数分别为：
		remote_storage：使用 remoteStorage.ApplyConfig 函数。
		web_handler：使用 webHandler.ApplyConfig 函数。
		query_engine：使用一个匿名函数，用于设置查询日志记录器。
		scrape：使用 scrapeManager.ApplyConfig 函数。
		scrape_sd：使用一个匿名函数，用于应用抓取服务发现配置。
		notify：使用 notifierManager.ApplyConfig 函数。
		notify_sd：使用一个匿名函数，用于应用通知服务发现配置。
		rules：使用一个匿名函数，用于更新规则管理器。
		tracing：使用 tracingManager.ApplyConfig 函数。
	*/
	//服务组件配置应用
	//除了服务组件ruleManager用的方法是Update，其他服务组件的在匿名函数中通过各自的ApplyConfig方法，实现配置的管理。
	// 所有的 服务都在这里配置，后续会有一个方法将所有都启动，所以看完main函数后，去看每一个模块的AppliConfig
	reloaders := []reloader{
		{
			name:     "db_storage",
			reloader: localStorage.ApplyConfig,
		}, {
			name:     "remote_storage",
			reloader: remoteStorage.ApplyConfig,
		}, {
			name:     "web_handler",
			reloader: webHandler.ApplyConfig,
		}, {
			name: "query_engine",
			reloader: func(cfg *config.Config) error {
				if agentMode {
					// No-op in Agent mode.
					return nil
				}

				if cfg.GlobalConfig.QueryLogFile == "" {
					queryEngine.SetQueryLogger(nil)
					return nil
				}

				l, err := logging.NewJSONFileLogger(cfg.GlobalConfig.QueryLogFile)
				if err != nil {
					return err
				}
				queryEngine.SetQueryLogger(l)
				return nil
			},
		}, {
			// The Scrape and notifier managers need to reload before the Discovery manager as
			// they need to read the most updated config when receiving the new targets list.
			name:     "scrape",
			reloader: scrapeManager.ApplyConfig,
		}, {
			name: "scrape_sd",
			reloader: func(cfg *config.Config) error {
				c := make(map[string]discovery.Configs)
				scfgs, err := cfg.GetScrapeConfigs()
				if err != nil {
					return err
				}
				for _, v := range scfgs {
					c[v.JobName] = v.ServiceDiscoveryConfigs
				}
				// 服务发现 配置
				return discoveryManagerScrape.ApplyConfig(c)
			},
		}, {
			name:     "notify",
			reloader: notifierManager.ApplyConfig,
		}, {
			name: "notify_sd",
			reloader: func(cfg *config.Config) error {
				c := make(map[string]discovery.Configs)
				for k, v := range cfg.AlertingConfig.AlertmanagerConfigs.ToMap() {
					c[k] = v.ServiceDiscoveryConfigs
				}
				return discoveryManagerNotify.ApplyConfig(c)
			},
		}, {
			name: "rules",
			reloader: func(cfg *config.Config) error {
				if agentMode {
					// No-op in Agent mode
					return nil
				}

				// Get all rule files matching the configuration paths.
				var files []string
				for _, pat := range cfg.RuleFiles {
					fs, err := filepath.Glob(pat)
					if err != nil {
						// The only error can be a bad pattern.
						return fmt.Errorf("error retrieving rule files for %s: %w", pat, err)
					}
					files = append(files, fs...)
				}
				return ruleManager.Update(
					time.Duration(cfg.GlobalConfig.EvaluationInterval),
					files,
					cfg.GlobalConfig.ExternalLabels,
					externalURL,
					nil,
				)
			},
		}, {
			name:     "tracing",
			reloader: tracingManager.ApplyConfig,
		},
	}

	prometheus.MustRegister(configSuccess)
	prometheus.MustRegister(configSuccessTime)

	// Start all components while we wait for TSDB to open but only load
	// initial config and mark ourselves as ready after it completed.
	/*
		首先，代码使用 prometheus.MustRegister 函数注册了 configSuccess 和 configSuccessTime 指标。
		接下来，代码创建了一个 dbOpen 通道，用于等待 TSDB 打开。
		然后，代码定义了一个 closeOnce 类型，用于封装一个只能关闭一次的通道。它包含一个 C 字段，表示通道；一个 once 字段，表示同步锁；以及一个 Close 函数，用于关闭通道。
		接下来，代码创建了一个 reloadReady 变量，用于等待服务器准备好处理重新加载。
		然后，代码使用 webHandler.Listener 函数获取监听器。如果发生错误，则在日志记录器上打印错误消息并退出程序。
		最后，代码使用 toolkit_web.Validate 函数验证 Web 配置文件。如果发生错误，则在日志记录器上打印错误消息并退出程序。
	*/
	dbOpen := make(chan struct{})

	// sync.Once is used to make sure we can close the channel at different execution stages(SIGTERM or when the config is loaded).
	type closeOnce struct {
		C     chan struct{}
		once  sync.Once
		Close func()
	}
	// Wait until the server is ready to handle reloading.
	reloadReady := &closeOnce{
		C: make(chan struct{}),
	}
	reloadReady.Close = func() {
		reloadReady.once.Do(func() {
			close(reloadReady.C)
		})
	}

	listener, err := webHandler.Listener()
	if err != nil {
		level.Error(logger).Log("msg", "Unable to start web listener", "err", err)
		os.Exit(1)
	}

	err = toolkit_web.Validate(*webConfig)
	if err != nil {
		level.Error(logger).Log("msg", "Unable to validate web configuration file", "err", err)
		os.Exit(1)
	}

	/*
		使用 run.Group 类型创建了一个 g 变量，用于管理一组并发运行的进程。
		首先，代码添加了一个终止处理程序。它监听 os.Interrupt 和 syscall.SIGTERM 信号，并在接收到信号时退出程序。
		接下来，代码添加了一个抓取发现管理器。它使用 discoveryManagerScrape.Run 函数启动抓取发现管理器，并在退出时调用 cancelScrape 函数停止抓取发现管理器。
		然后，代码添加了一个通知发现管理器。它使用 discoveryManagerNotify.Run 函数启动通知发现管理器，并在退出时调用 cancelNotify 函数停止通知发现管理器。
		接下来，代码检查是否处于代理模式。如果不是，则添加一个规则管理器。它等待重新加载就绪，然后使用 ruleManager.Run 函数启动规则管理器，并在退出时调用 ruleManager.Stop 函数停止规则管理器。
		然后，代码添加了一个抓取管理器。它等待重新加载就绪，然后使用 scrapeManager.Run 函数启动抓取管理器，并在退出时调用 scrapeManager.Stop 函数停止抓取管理器。
		接下来，代码添加了一个追踪管理器。它等待重新加载就绪，然后使用 tracingManager.Run 函数启动追踪管理器，并在退出时调用 tracingManager.Stop 函数停止追踪管理器。
		最后，代码添加了一个重新加载处理程序。它监听 syscall.SIGHUP 信号和 Web 处理程序的重新加载通道，并在接收到信号或消息时重新加载配置。它使用 reloadConfig 函数重新加载配置。
	*/
	var g run.Group
	{
		// Termination handler.
		term := make(chan os.Signal, 1)
		signal.Notify(term, os.Interrupt, syscall.SIGTERM)
		cancel := make(chan struct{})
		g.Add(
			func() error {
				// Don't forget to release the reloadReady channel so that waiting blocks can exit normally.
				select {
				case <-term:
					level.Warn(logger).Log("msg", "Received SIGTERM, exiting gracefully...")
					reloadReady.Close()
				case <-webHandler.Quit():
					level.Warn(logger).Log("msg", "Received termination request via web service, exiting gracefully...")
				case <-cancel:
					reloadReady.Close()
				}
				return nil
			},
			func(err error) {
				close(cancel)
				webHandler.SetReady(false)
			},
		)
	}
	{
		// Scrape discovery manager.
		g.Add(
			func() error {
				err := discoveryManagerScrape.Run()
				level.Info(logger).Log("msg", "Scrape discovery manager stopped")
				return err
			},
			func(err error) {
				level.Info(logger).Log("msg", "Stopping scrape discovery manager...")
				cancelScrape()
			},
		)
	}
	{
		// Notify discovery manager.
		g.Add(
			func() error {
				err := discoveryManagerNotify.Run()
				level.Info(logger).Log("msg", "Notify discovery manager stopped")
				return err
			},
			func(err error) {
				level.Info(logger).Log("msg", "Stopping notify discovery manager...")
				cancelNotify()
			},
		)
	}
	if !agentMode {
		// Rule manager.
		g.Add(
			func() error {
				<-reloadReady.C
				ruleManager.Run()
				return nil
			},
			func(err error) {
				ruleManager.Stop()
			},
		)
	}
	{
		// Scrape manager.
		g.Add(
			func() error {
				// When the scrape manager receives a new targets list
				// it needs to read a valid config for each job.
				// It depends on the config being in sync with the discovery manager so
				// we wait until the config is fully loaded.
				<-reloadReady.C

				err := scrapeManager.Run(discoveryManagerScrape.SyncCh())
				level.Info(logger).Log("msg", "Scrape manager stopped")
				return err
			},
			func(err error) {
				// Scrape manager needs to be stopped before closing the local TSDB
				// so that it doesn't try to write samples to a closed storage.
				// We should also wait for rule manager to be fully stopped to ensure
				// we don't trigger any false positive alerts for rules using absent().
				level.Info(logger).Log("msg", "Stopping scrape manager...")
				scrapeManager.Stop()
			},
		)
	}
	{
		// Tracing manager.
		g.Add(
			func() error {
				<-reloadReady.C
				tracingManager.Run()
				return nil
			},
			func(err error) {
				tracingManager.Stop()
			},
		)
	}
	{
		// Reload handler.

		// Make sure that sighup handler is registered with a redirect to the channel before the potentially
		// long and synchronous tsdb init.
		hup := make(chan os.Signal, 1)
		signal.Notify(hup, syscall.SIGHUP)
		cancel := make(chan struct{})
		g.Add(
			func() error {
				<-reloadReady.C

				for {
					select {
					case <-hup:
						if err := reloadConfig(cfg.configFile, cfg.enableExpandExternalLabels, cfg.tsdb.EnableExemplarStorage, logger, noStepSubqueryInterval, reloaders...); err != nil {
							level.Error(logger).Log("msg", "Error reloading config", "err", err)
						}
					case rc := <-webHandler.Reload():
						if err := reloadConfig(cfg.configFile, cfg.enableExpandExternalLabels, cfg.tsdb.EnableExemplarStorage, logger, noStepSubqueryInterval, reloaders...); err != nil {
							level.Error(logger).Log("msg", "Error reloading config", "err", err)
							rc <- err
						} else {
							rc <- nil
						}
					case <-cancel:
						return nil
					}
				}
			},
			func(err error) {
				// Wait for any in-progress reloads to complete to avoid
				// reloading things after they have been shutdown.
				cancel <- struct{}{}
			},
		)
	}

	/*
		这段代码继续使用 run.Group 类型添加更多的进程。
		首先，代码添加了一个初始配置加载程序。它等待 TSDB 打开，然后使用 reloadConfig 函数重新加载配置。如果重新加载成功，则关闭 reloadReady 通道，并将 Web 处理程序标记为准备就绪。
		接下来，代码检查是否处于代理模式。如果不是，则添加一个 TSDB 进程。它使用 openDBWithMetrics 函数打开本地存储，并在退出时调用 fanoutStorage.Close 函数关闭存储。
		如果处于代理模式，则添加一个 WAL 存储进程。它使用 agent.Open 函数打开本地存储，并在退出时调用 fanoutStorage.Close 函数关闭存储。
		然后，代码添加了一个 Web 处理程序进程。它使用 webHandler.Run 函数启动 Web 服务器，并在退出时调用 cancelWeb 函数取消 Web 上下文。
		接下来，代码添加了一个通知器进程。它等待重新加载就绪，然后使用 notifierManager.Run 函数启动通知器管理器，并在退出时调用 notifierManager.Stop 函数停止通知器管理器。
		最后，代码使用 g.Run 函数启动所有进程。如果发生错误，则在日志记录器上打印错误消息并退出程序。
	*/
	{
		// Initial configuration loading.
		cancel := make(chan struct{})
		g.Add(
			func() error {
				select {
				case <-dbOpen:
				// In case a shutdown is initiated before the dbOpen is released
				case <-cancel:
					reloadReady.Close()
					return nil
				}

				if err := reloadConfig(cfg.configFile, cfg.enableExpandExternalLabels, cfg.tsdb.EnableExemplarStorage, logger, noStepSubqueryInterval, reloaders...); err != nil {
					return fmt.Errorf("error loading config from %q: %w", cfg.configFile, err)
				}

				reloadReady.Close()

				webHandler.SetReady(true)
				level.Info(logger).Log("msg", "Server is ready to receive web requests.")
				<-cancel
				return nil
			},
			func(err error) {
				close(cancel)
			},
		)
	}
	if !agentMode {
		// TSDB.
		opts := cfg.tsdb.ToTSDBOptions()
		cancel := make(chan struct{})
		g.Add(
			func() error {
				level.Info(logger).Log("msg", "Starting TSDB ...")
				if cfg.tsdb.WALSegmentSize != 0 {
					if cfg.tsdb.WALSegmentSize < 10*1024*1024 || cfg.tsdb.WALSegmentSize > 256*1024*1024 {
						return errors.New("flag 'storage.tsdb.wal-segment-size' must be set between 10MB and 256MB")
					}
				}
				if cfg.tsdb.MaxBlockChunkSegmentSize != 0 {
					if cfg.tsdb.MaxBlockChunkSegmentSize < 1024*1024 {
						return errors.New("flag 'storage.tsdb.max-block-chunk-segment-size' must be set over 1MB")
					}
				}

				db, err := openDBWithMetrics(localStoragePath, logger, prometheus.DefaultRegisterer, &opts, localStorage.getStats())
				if err != nil {
					return fmt.Errorf("opening storage failed: %w", err)
				}

				switch fsType := prom_runtime.Statfs(localStoragePath); fsType {
				case "NFS_SUPER_MAGIC":
					level.Warn(logger).Log("fs_type", fsType, "msg", "This filesystem is not supported and may lead to data corruption and data loss. Please carefully read https://prometheus.io/docs/prometheus/latest/storage/ to learn more about supported filesystems.")
				default:
					level.Info(logger).Log("fs_type", fsType)
				}

				level.Info(logger).Log("msg", "TSDB started")
				level.Debug(logger).Log("msg", "TSDB options",
					"MinBlockDuration", cfg.tsdb.MinBlockDuration,
					"MaxBlockDuration", cfg.tsdb.MaxBlockDuration,
					"MaxBytes", cfg.tsdb.MaxBytes,
					"NoLockfile", cfg.tsdb.NoLockfile,
					"RetentionDuration", cfg.tsdb.RetentionDuration,
					"WALSegmentSize", cfg.tsdb.WALSegmentSize,
					"WALCompression", cfg.tsdb.WALCompression,
				)

				startTimeMargin := int64(2 * time.Duration(cfg.tsdb.MinBlockDuration).Seconds() * 1000)
				localStorage.Set(db, startTimeMargin)
				db.SetWriteNotified(remoteStorage)
				close(dbOpen)
				<-cancel
				return nil
			},
			func(err error) {
				if err := fanoutStorage.Close(); err != nil {
					level.Error(logger).Log("msg", "Error stopping storage", "err", err)
				}
				close(cancel)
			},
		)
	}
	if agentMode {
		// WAL storage.
		opts := cfg.agent.ToAgentOptions()
		cancel := make(chan struct{})
		g.Add(
			func() error {
				level.Info(logger).Log("msg", "Starting WAL storage ...")
				if cfg.agent.WALSegmentSize != 0 {
					if cfg.agent.WALSegmentSize < 10*1024*1024 || cfg.agent.WALSegmentSize > 256*1024*1024 {
						return errors.New("flag 'storage.agent.wal-segment-size' must be set between 10MB and 256MB")
					}
				}
				db, err := agent.Open(
					logger,
					prometheus.DefaultRegisterer,
					remoteStorage,
					localStoragePath,
					&opts,
				)
				if err != nil {
					return fmt.Errorf("opening storage failed: %w", err)
				}

				switch fsType := prom_runtime.Statfs(localStoragePath); fsType {
				case "NFS_SUPER_MAGIC":
					level.Warn(logger).Log("fs_type", fsType, "msg", "This filesystem is not supported and may lead to data corruption and data loss. Please carefully read https://prometheus.io/docs/prometheus/latest/storage/ to learn more about supported filesystems.")
				default:
					level.Info(logger).Log("fs_type", fsType)
				}

				level.Info(logger).Log("msg", "Agent WAL storage started")
				level.Debug(logger).Log("msg", "Agent WAL storage options",
					"WALSegmentSize", cfg.agent.WALSegmentSize,
					"WALCompression", cfg.agent.WALCompression,
					"StripeSize", cfg.agent.StripeSize,
					"TruncateFrequency", cfg.agent.TruncateFrequency,
					"MinWALTime", cfg.agent.MinWALTime,
					"MaxWALTime", cfg.agent.MaxWALTime,
				)

				localStorage.Set(db, 0)
				close(dbOpen)
				<-cancel
				return nil
			},
			func(e error) {
				if err := fanoutStorage.Close(); err != nil {
					level.Error(logger).Log("msg", "Error stopping storage", "err", err)
				}
				close(cancel)
			},
		)
	}
	{
		// Web handler.
		g.Add(
			func() error {
				if err := webHandler.Run(ctxWeb, listener, *webConfig); err != nil {
					return fmt.Errorf("error starting web server: %w", err)
				}
				return nil
			},
			func(err error) {
				cancelWeb()
			},
		)
	}
	{
		// Notifier.

		// Calling notifier.Stop() before ruleManager.Stop() will cause a panic if the ruleManager isn't running,
		// so keep this interrupt after the ruleManager.Stop().
		g.Add(
			func() error {
				// When the notifier manager receives a new targets list
				// it needs to read a valid config for each job.
				// It depends on the config being in sync with the discovery manager
				// so we wait until the config is fully loaded.
				<-reloadReady.C

				notifierManager.Run(discoveryManagerNotify.SyncCh())
				level.Info(logger).Log("msg", "Notifier manager stopped")
				return nil
			},
			func(err error) {
				notifierManager.Stop()
			},
		)
	}
	if err := g.Run(); err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
	level.Info(logger).Log("msg", "See you next time!")
}

func openDBWithMetrics(dir string, logger log.Logger, reg prometheus.Registerer, opts *tsdb.Options, stats *tsdb.DBStats) (*tsdb.DB, error) {
	db, err := tsdb.Open(
		dir,
		log.With(logger, "component", "tsdb"),
		reg,
		opts,
		stats,
	)
	if err != nil {
		return nil, err
	}

	reg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_lowest_timestamp_seconds",
			Help: "Lowest timestamp value stored in the database.",
		}, func() float64 {
			bb := db.Blocks()
			if len(bb) == 0 {
				return float64(db.Head().MinTime() / 1000)
			}
			return float64(db.Blocks()[0].Meta().MinTime / 1000)
		}), prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_head_min_time_seconds",
			Help: "Minimum time bound of the head block.",
		}, func() float64 { return float64(db.Head().MinTime() / 1000) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_head_max_time_seconds",
			Help: "Maximum timestamp of the head block.",
		}, func() float64 { return float64(db.Head().MaxTime() / 1000) }),
	)

	return db, nil
}

type safePromQLNoStepSubqueryInterval struct {
	value atomic.Int64
}

func durationToInt64Millis(d time.Duration) int64 {
	return int64(d / time.Millisecond)
}

func (i *safePromQLNoStepSubqueryInterval) Set(ev model.Duration) {
	i.value.Store(durationToInt64Millis(time.Duration(ev)))
}

func (i *safePromQLNoStepSubqueryInterval) Get(int64) int64 {
	return i.value.Load()
}

type reloader struct {
	name     string
	reloader func(*config.Config) error
}

func reloadConfig(filename string, expandExternalLabels, enableExemplarStorage bool, logger log.Logger, noStepSuqueryInterval *safePromQLNoStepSubqueryInterval, rls ...reloader) (err error) {
	start := time.Now()
	timings := []interface{}{}
	level.Info(logger).Log("msg", "Loading configuration file", "filename", filename)

	defer func() {
		if err == nil {
			configSuccess.Set(1)
			configSuccessTime.SetToCurrentTime()
		} else {
			configSuccess.Set(0)
		}
	}()

	conf, err := config.LoadFile(filename, agentMode, expandExternalLabels, logger)
	if err != nil {
		return fmt.Errorf("couldn't load configuration (--config.file=%q): %w", filename, err)
	}

	if enableExemplarStorage {
		if conf.StorageConfig.ExemplarsConfig == nil {
			conf.StorageConfig.ExemplarsConfig = &config.DefaultExemplarsConfig
		}
	}

	failed := false
	for _, rl := range rls {
		rstart := time.Now()
		if err := rl.reloader(conf); err != nil {
			level.Error(logger).Log("msg", "Failed to apply configuration", "err", err)
			failed = true
		}
		timings = append(timings, rl.name, time.Since(rstart))
	}
	if failed {
		return fmt.Errorf("one or more errors occurred while applying the new configuration (--config.file=%q)", filename)
	}

	noStepSuqueryInterval.Set(conf.GlobalConfig.EvaluationInterval)
	l := []interface{}{"msg", "Completed loading of configuration file", "filename", filename, "totalDuration", time.Since(start)}
	level.Info(logger).Log(append(l, timings...)...)
	return nil
}

func startsOrEndsWithQuote(s string) bool {
	return strings.HasPrefix(s, "\"") || strings.HasPrefix(s, "'") ||
		strings.HasSuffix(s, "\"") || strings.HasSuffix(s, "'")
}

// compileCORSRegexString compiles given string and adds anchors
func compileCORSRegexString(s string) (*regexp.Regexp, error) {
	r, err := relabel.NewRegexp(s)
	if err != nil {
		return nil, err
	}
	return r.Regexp, nil
}

// computeExternalURL computes a sanitized external URL from a raw input. It infers unset
// URL parts from the OS and the given listen address.
func computeExternalURL(u, listenAddr string) (*url.URL, error) {
	if u == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		_, port, err := net.SplitHostPort(listenAddr)
		if err != nil {
			return nil, err
		}
		u = fmt.Sprintf("http://%s:%s/", hostname, port)
	}

	if startsOrEndsWithQuote(u) {
		return nil, errors.New("URL must not begin or end with quotes")
	}

	eu, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	ppref := strings.TrimRight(eu.Path, "/")
	if ppref != "" && !strings.HasPrefix(ppref, "/") {
		ppref = "/" + ppref
	}
	eu.Path = ppref

	return eu, nil
}

// readyStorage implements the Storage interface while allowing to set the actual
// storage at a later point in time.
type readyStorage struct {
	mtx             sync.RWMutex
	db              storage.Storage
	startTimeMargin int64
	stats           *tsdb.DBStats
}

func (s *readyStorage) ApplyConfig(conf *config.Config) error {
	db := s.get()
	if db, ok := db.(*tsdb.DB); ok {
		return db.ApplyConfig(conf)
	}
	return nil
}

// Set the storage.
func (s *readyStorage) Set(db storage.Storage, startTimeMargin int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.db = db
	s.startTimeMargin = startTimeMargin
}

func (s *readyStorage) get() storage.Storage {
	s.mtx.RLock()
	x := s.db
	s.mtx.RUnlock()
	return x
}

func (s *readyStorage) getStats() *tsdb.DBStats {
	s.mtx.RLock()
	x := s.stats
	s.mtx.RUnlock()
	return x
}

// StartTime implements the Storage interface.
func (s *readyStorage) StartTime() (int64, error) {
	if x := s.get(); x != nil {
		switch db := x.(type) {
		case *tsdb.DB:
			var startTime int64
			if len(db.Blocks()) > 0 {
				startTime = db.Blocks()[0].Meta().MinTime
			} else {
				startTime = time.Now().Unix() * 1000
			}
			// Add a safety margin as it may take a few minutes for everything to spin up.
			return startTime + s.startTimeMargin, nil
		case *agent.DB:
			return db.StartTime()
		default:
			panic(fmt.Sprintf("unknown storage type %T", db))
		}
	}

	return math.MaxInt64, tsdb.ErrNotReady
}

// Querier implements the Storage interface.
func (s *readyStorage) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	if x := s.get(); x != nil {
		return x.Querier(ctx, mint, maxt)
	}
	return nil, tsdb.ErrNotReady
}

// ChunkQuerier implements the Storage interface.
func (s *readyStorage) ChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	if x := s.get(); x != nil {
		return x.ChunkQuerier(ctx, mint, maxt)
	}
	return nil, tsdb.ErrNotReady
}

func (s *readyStorage) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	if x := s.get(); x != nil {
		switch db := x.(type) {
		case *tsdb.DB:
			return db.ExemplarQuerier(ctx)
		case *agent.DB:
			return nil, agent.ErrUnsupported
		default:
			panic(fmt.Sprintf("unknown storage type %T", db))
		}
	}
	return nil, tsdb.ErrNotReady
}

// Appender implements the Storage interface.
func (s *readyStorage) Appender(ctx context.Context) storage.Appender {
	if x := s.get(); x != nil {
		return x.Appender(ctx)
	}
	return notReadyAppender{}
}

type notReadyAppender struct{}

func (n notReadyAppender) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	return 0, tsdb.ErrNotReady
}

func (n notReadyAppender) AppendExemplar(ref storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, tsdb.ErrNotReady
}

func (n notReadyAppender) AppendHistogram(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, tsdb.ErrNotReady
}

func (n notReadyAppender) UpdateMetadata(ref storage.SeriesRef, l labels.Labels, m metadata.Metadata) (storage.SeriesRef, error) {
	return 0, tsdb.ErrNotReady
}

func (n notReadyAppender) Commit() error { return tsdb.ErrNotReady }

func (n notReadyAppender) Rollback() error { return tsdb.ErrNotReady }

// Close implements the Storage interface.
func (s *readyStorage) Close() error {
	if x := s.get(); x != nil {
		return x.Close()
	}
	return nil
}

// CleanTombstones implements the api_v1.TSDBAdminStats and api_v2.TSDBAdmin interfaces.
func (s *readyStorage) CleanTombstones() error {
	if x := s.get(); x != nil {
		switch db := x.(type) {
		case *tsdb.DB:
			return db.CleanTombstones()
		case *agent.DB:
			return agent.ErrUnsupported
		default:
			panic(fmt.Sprintf("unknown storage type %T", db))
		}
	}
	return tsdb.ErrNotReady
}

// Delete implements the api_v1.TSDBAdminStats and api_v2.TSDBAdmin interfaces.
func (s *readyStorage) Delete(mint, maxt int64, ms ...*labels.Matcher) error {
	if x := s.get(); x != nil {
		switch db := x.(type) {
		case *tsdb.DB:
			return db.Delete(mint, maxt, ms...)
		case *agent.DB:
			return agent.ErrUnsupported
		default:
			panic(fmt.Sprintf("unknown storage type %T", db))
		}
	}
	return tsdb.ErrNotReady
}

// Snapshot implements the api_v1.TSDBAdminStats and api_v2.TSDBAdmin interfaces.
func (s *readyStorage) Snapshot(dir string, withHead bool) error {
	if x := s.get(); x != nil {
		switch db := x.(type) {
		case *tsdb.DB:
			return db.Snapshot(dir, withHead)
		case *agent.DB:
			return agent.ErrUnsupported
		default:
			panic(fmt.Sprintf("unknown storage type %T", db))
		}
	}
	return tsdb.ErrNotReady
}

// Stats implements the api_v1.TSDBAdminStats interface.
func (s *readyStorage) Stats(statsByLabelName string, limit int) (*tsdb.Stats, error) {
	if x := s.get(); x != nil {
		switch db := x.(type) {
		case *tsdb.DB:
			return db.Head().Stats(statsByLabelName, limit), nil
		case *agent.DB:
			return nil, agent.ErrUnsupported
		default:
			panic(fmt.Sprintf("unknown storage type %T", db))
		}
	}
	return nil, tsdb.ErrNotReady
}

// WALReplayStatus implements the api_v1.TSDBStats interface.
func (s *readyStorage) WALReplayStatus() (tsdb.WALReplayStatus, error) {
	if x := s.getStats(); x != nil {
		return x.Head.WALReplayStatus.GetWALReplayStatus(), nil
	}
	return tsdb.WALReplayStatus{}, tsdb.ErrNotReady
}

// ErrNotReady is returned if the underlying scrape manager is not ready yet.
var ErrNotReady = errors.New("Scrape manager not ready")

// ReadyScrapeManager allows a scrape manager to be retrieved. Even if it's set at a later point in time.
type readyScrapeManager struct {
	mtx sync.RWMutex
	m   *scrape.Manager
}

// Set the scrape manager.
func (rm *readyScrapeManager) Set(m *scrape.Manager) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()

	rm.m = m
}

// Get the scrape manager. If is not ready, return an error.
func (rm *readyScrapeManager) Get() (*scrape.Manager, error) {
	rm.mtx.RLock()
	defer rm.mtx.RUnlock()

	if rm.m != nil {
		return rm.m, nil
	}

	return nil, ErrNotReady
}

// tsdbOptions is tsdb.Option version with defined units.
// This is required as tsdb.Option fields are unit agnostic (time).
type tsdbOptions struct {
	WALSegmentSize                 units.Base2Bytes
	MaxBlockChunkSegmentSize       units.Base2Bytes
	RetentionDuration              model.Duration
	MaxBytes                       units.Base2Bytes
	NoLockfile                     bool
	WALCompression                 bool
	HeadChunksWriteQueueSize       int
	SamplesPerChunk                int
	StripeSize                     int
	MinBlockDuration               model.Duration
	MaxBlockDuration               model.Duration
	OutOfOrderTimeWindow           int64
	EnableExemplarStorage          bool
	MaxExemplars                   int64
	EnableMemorySnapshotOnShutdown bool
	EnableNativeHistograms         bool
}

func (opts tsdbOptions) ToTSDBOptions() tsdb.Options {
	return tsdb.Options{
		WALSegmentSize:                 int(opts.WALSegmentSize),
		MaxBlockChunkSegmentSize:       int64(opts.MaxBlockChunkSegmentSize),
		RetentionDuration:              int64(time.Duration(opts.RetentionDuration) / time.Millisecond),
		MaxBytes:                       int64(opts.MaxBytes),
		NoLockfile:                     opts.NoLockfile,
		AllowOverlappingCompaction:     true,
		WALCompression:                 opts.WALCompression,
		HeadChunksWriteQueueSize:       opts.HeadChunksWriteQueueSize,
		SamplesPerChunk:                opts.SamplesPerChunk,
		StripeSize:                     opts.StripeSize,
		MinBlockDuration:               int64(time.Duration(opts.MinBlockDuration) / time.Millisecond),
		MaxBlockDuration:               int64(time.Duration(opts.MaxBlockDuration) / time.Millisecond),
		EnableExemplarStorage:          opts.EnableExemplarStorage,
		MaxExemplars:                   opts.MaxExemplars,
		EnableMemorySnapshotOnShutdown: opts.EnableMemorySnapshotOnShutdown,
		EnableNativeHistograms:         opts.EnableNativeHistograms,
		OutOfOrderTimeWindow:           opts.OutOfOrderTimeWindow,
	}
}

// agentOptions is a version of agent.Options with defined units. This is required
// as agent.Option fields are unit agnostic (time).
type agentOptions struct {
	WALSegmentSize         units.Base2Bytes
	WALCompression         bool
	StripeSize             int
	TruncateFrequency      model.Duration
	MinWALTime, MaxWALTime model.Duration
	NoLockfile             bool
}

func (opts agentOptions) ToAgentOptions() agent.Options {
	return agent.Options{
		WALSegmentSize:    int(opts.WALSegmentSize),
		WALCompression:    opts.WALCompression,
		StripeSize:        opts.StripeSize,
		TruncateFrequency: time.Duration(opts.TruncateFrequency),
		MinWALTime:        durationToInt64Millis(time.Duration(opts.MinWALTime)),
		MaxWALTime:        durationToInt64Millis(time.Duration(opts.MaxWALTime)),
		NoLockfile:        opts.NoLockfile,
	}
}

// discoveryManager interfaces the discovery manager. This is used to keep using
// the manager that restarts SD's on reload for a few releases until we feel
// the new manager can be enabled for all users.
type discoveryManager interface {
	ApplyConfig(cfg map[string]discovery.Configs) error
	Run() error
	SyncCh() <-chan map[string][]*targetgroup.Group
}
