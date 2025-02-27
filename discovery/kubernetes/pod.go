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

package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/util/strutil"
)

const nodeIndex = "node"

var (
	podAddCount    = eventCount.WithLabelValues("pod", "add")
	podUpdateCount = eventCount.WithLabelValues("pod", "update")
	podDeleteCount = eventCount.WithLabelValues("pod", "delete")
)

// Pod discovers new pod targets.
type Pod struct {
	podInf           cache.SharedIndexInformer
	nodeInf          cache.SharedInformer
	withNodeMetadata bool
	store            cache.Store
	logger           log.Logger
	queue            *workqueue.Type
}

// NewPod creates a new pod discovery.
func NewPod(l log.Logger, pods cache.SharedIndexInformer, nodes cache.SharedInformer) *Pod {
	if l == nil {
		l = log.NewNopLogger()
	}

	p := &Pod{
		podInf:           pods,
		nodeInf:          nodes,
		withNodeMetadata: nodes != nil,
		store:            pods.GetStore(),
		logger:           l,
		queue:            workqueue.NewNamed("pod"),
	}
	// 这里的 podAddCount、podDeleteCount和podUpdateCount分别对应下面三个指标序列，指标含义也比较明显：
	//
	//prometheus_sd_kubernetes_events_total(role="pod", event="add")
	//
	//prometheus_sd_kubernetes_events_total(role="pod", event="delete")
	//
	//prometheus_sd_kubernetes_events_total(role="pod", event="update")
	//
	//role标识资源类型，包括："endpointslice", "endpoints", "node", "pod", "service", "ingress"五种类型；
	//
	//event标识事件类型，包括："add", "delete", "update"三种类型。
	// 注册资源事件，分别对应资源创建、资源删除和资源更新事件处理；
	_, err := p.podInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(o interface{}) {
			podAddCount.Inc()
			p.enqueue(o)
		},
		DeleteFunc: func(o interface{}) {
			podDeleteCount.Inc()
			p.enqueue(o)
		},
		UpdateFunc: func(_, o interface{}) {
			podUpdateCount.Inc()
			p.enqueue(o)
		},
	})
	if err != nil {
		level.Error(l).Log("msg", "Error adding pods event handler.", "err", err)
	}

	if p.withNodeMetadata {
		_, err = p.nodeInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(o interface{}) {
				node := o.(*apiv1.Node)
				p.enqueuePodsForNode(node.Name)
			},
			UpdateFunc: func(_, o interface{}) {
				node := o.(*apiv1.Node)
				p.enqueuePodsForNode(node.Name)
			},
			DeleteFunc: func(o interface{}) {
				node := o.(*apiv1.Node)
				p.enqueuePodsForNode(node.Name)
			},
		})
		if err != nil {
			level.Error(l).Log("msg", "Error adding pods event handler.", "err", err)
		}
	}

	return p
}

func (p *Pod) enqueue(obj interface{}) {
	// 获取资源对象的Key
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		return
	}

	// 将资源对象的key写入到WorkQueue中
	p.queue.Add(key)
}

// Run implements the Discoverer interface.
func (p *Pod) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	defer p.queue.ShutDown()

	cacheSyncs := []cache.InformerSynced{p.podInf.HasSynced}
	if p.withNodeMetadata {
		cacheSyncs = append(cacheSyncs, p.nodeInf.HasSynced)
	}

	if !cache.WaitForCacheSync(ctx.Done(), cacheSyncs...) {
		if !errors.Is(ctx.Err(), context.Canceled) {
			level.Error(p.logger).Log("msg", "pod informer unable to sync cache")
		}
		return
	}

	go func() {
		for p.process(ctx, ch) { // nolint:revive
		}
	}()

	// Block until the target provider is explicitly canceled.
	<-ctx.Done()
}

// 对于POD资源，这里的key就是：namespace/pod_name格式，比如key=test01/nginx-deployment-5ffc5bf56c-n2pl8。
func (p *Pod) process(ctx context.Context, ch chan<- []*targetgroup.Group) bool {
	// workqueue中取出key
	keyObj, quit := p.queue.Get()
	if quit {
		return false
	}
	defer p.queue.Done(keyObj)
	key := keyObj.(string)
	// 从key解析出namespce和Podname
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return true
	}
	// 基于key从本读缓存中查询出对象信息
	o, exists, err := p.store.GetByKey(key)
	if err != nil {
		return true
	}
	if !exists {
		// 如果key在本地缓存不存在，则表示当前对象删除，这里则发送一个空的采集点集合，目的是将之前基于该pod生成的所有采集点覆盖掉
		send(ctx, ch, &targetgroup.Group{Source: podSourceFromNamespaceAndName(namespace, name)})
		return true
	}
	pod, err := convertToPod(o)
	if err != nil {
		level.Error(p.logger).Log("msg", "converting to Pod object failed", "err", err)
		return true
	}
	// 基于Pod信息，使用buildPod（）方法构建出采集点，然后send函数中，将这些采集点信息发送到ch通道上，这样就能贝updater协程监听到。
	// 针对Pod里的每个Container上的每个port，都会生成一个对应采集点target，其中__address__就是PodIP+port组合。
	send(ctx, ch, p.buildPod(pod))
	return true
}

func convertToPod(o interface{}) (*apiv1.Pod, error) {
	pod, ok := o.(*apiv1.Pod)
	if ok {
		return pod, nil
	}

	return nil, fmt.Errorf("received unexpected object: %v", o)
}

const (
	podNameLabel                  = metaLabelPrefix + "pod_name"
	podIPLabel                    = metaLabelPrefix + "pod_ip"
	podContainerNameLabel         = metaLabelPrefix + "pod_container_name"
	podContainerIDLabel           = metaLabelPrefix + "pod_container_id"
	podContainerImageLabel        = metaLabelPrefix + "pod_container_image"
	podContainerPortNameLabel     = metaLabelPrefix + "pod_container_port_name"
	podContainerPortNumberLabel   = metaLabelPrefix + "pod_container_port_number"
	podContainerPortProtocolLabel = metaLabelPrefix + "pod_container_port_protocol"
	podContainerIsInit            = metaLabelPrefix + "pod_container_init"
	podReadyLabel                 = metaLabelPrefix + "pod_ready"
	podPhaseLabel                 = metaLabelPrefix + "pod_phase"
	podLabelPrefix                = metaLabelPrefix + "pod_label_"
	podLabelPresentPrefix         = metaLabelPrefix + "pod_labelpresent_"
	podAnnotationPrefix           = metaLabelPrefix + "pod_annotation_"
	podAnnotationPresentPrefix    = metaLabelPrefix + "pod_annotationpresent_"
	podNodeNameLabel              = metaLabelPrefix + "pod_node_name"
	podHostIPLabel                = metaLabelPrefix + "pod_host_ip"
	podUID                        = metaLabelPrefix + "pod_uid"
	podControllerKind             = metaLabelPrefix + "pod_controller_kind"
	podControllerName             = metaLabelPrefix + "pod_controller_name"
)

// GetControllerOf returns a pointer to a copy of the controllerRef if controllee has a controller
// https://github.com/kubernetes/apimachinery/blob/cd2cae2b39fa57e8063fa1f5f13cfe9862db3d41/pkg/apis/meta/v1/controller_ref.go
func GetControllerOf(controllee metav1.Object) *metav1.OwnerReference {
	for _, ref := range controllee.GetOwnerReferences() {
		if ref.Controller != nil && *ref.Controller {
			return &ref
		}
	}
	return nil
}

func podLabels(pod *apiv1.Pod) model.LabelSet {
	ls := model.LabelSet{
		podNameLabel:     lv(pod.ObjectMeta.Name),
		podIPLabel:       lv(pod.Status.PodIP),
		podReadyLabel:    podReady(pod),
		podPhaseLabel:    lv(string(pod.Status.Phase)),
		podNodeNameLabel: lv(pod.Spec.NodeName),
		podHostIPLabel:   lv(pod.Status.HostIP),
		podUID:           lv(string(pod.ObjectMeta.UID)),
	}

	createdBy := GetControllerOf(pod)
	if createdBy != nil {
		if createdBy.Kind != "" {
			ls[podControllerKind] = lv(createdBy.Kind)
		}
		if createdBy.Name != "" {
			ls[podControllerName] = lv(createdBy.Name)
		}
	}

	for k, v := range pod.Labels {
		ln := strutil.SanitizeLabelName(k)
		ls[model.LabelName(podLabelPrefix+ln)] = lv(v)
		ls[model.LabelName(podLabelPresentPrefix+ln)] = presentValue
	}

	for k, v := range pod.Annotations {
		ln := strutil.SanitizeLabelName(k)
		ls[model.LabelName(podAnnotationPrefix+ln)] = lv(v)
		ls[model.LabelName(podAnnotationPresentPrefix+ln)] = presentValue
	}

	return ls
}

func (p *Pod) findPodContainerStatus(statuses *[]apiv1.ContainerStatus, containerName string) (*apiv1.ContainerStatus, error) {
	for _, s := range *statuses {
		if s.Name == containerName {
			return &s, nil
		}
	}
	return nil, fmt.Errorf("cannot find container with name %v", containerName)
}

func (p *Pod) findPodContainerID(statuses *[]apiv1.ContainerStatus, containerName string) string {
	cStatus, err := p.findPodContainerStatus(statuses, containerName)
	if err != nil {
		level.Debug(p.logger).Log("msg", "cannot find container ID", "err", err)
		return ""
	}
	return cStatus.ContainerID
}

func (p *Pod) buildPod(pod *apiv1.Pod) *targetgroup.Group {
	tg := &targetgroup.Group{
		Source: podSource(pod),
	}
	// PodIP can be empty when a pod is starting or has been evicted.
	if len(pod.Status.PodIP) == 0 {
		return tg
	}

	tg.Labels = podLabels(pod)
	tg.Labels[namespaceLabel] = lv(pod.Namespace)
	if p.withNodeMetadata {
		tg.Labels = addNodeLabels(tg.Labels, p.nodeInf, p.logger, &pod.Spec.NodeName)
	}

	containers := append(pod.Spec.Containers, pod.Spec.InitContainers...)
	for i, c := range containers {
		isInit := i >= len(pod.Spec.Containers)

		cStatuses := &pod.Status.ContainerStatuses
		if isInit {
			cStatuses = &pod.Status.InitContainerStatuses
		}
		cID := p.findPodContainerID(cStatuses, c.Name)

		// If no ports are defined for the container, create an anonymous
		// target per container.
		if len(c.Ports) == 0 {
			// We don't have a port so we just set the address label to the pod IP.
			// The user has to add a port manually.
			tg.Targets = append(tg.Targets, model.LabelSet{
				model.AddressLabel:     lv(pod.Status.PodIP),
				podContainerNameLabel:  lv(c.Name),
				podContainerIDLabel:    lv(cID),
				podContainerImageLabel: lv(c.Image),
				podContainerIsInit:     lv(strconv.FormatBool(isInit)),
			})
			continue
		}
		// Otherwise create one target for each container/port combination.
		for _, port := range c.Ports {
			ports := strconv.FormatUint(uint64(port.ContainerPort), 10)
			addr := net.JoinHostPort(pod.Status.PodIP, ports)

			tg.Targets = append(tg.Targets, model.LabelSet{
				model.AddressLabel:            lv(addr),
				podContainerNameLabel:         lv(c.Name),
				podContainerIDLabel:           lv(cID),
				podContainerImageLabel:        lv(c.Image),
				podContainerPortNumberLabel:   lv(ports),
				podContainerPortNameLabel:     lv(port.Name),
				podContainerPortProtocolLabel: lv(string(port.Protocol)),
				podContainerIsInit:            lv(strconv.FormatBool(isInit)),
			})
		}
	}

	return tg
}

func (p *Pod) enqueuePodsForNode(nodeName string) {
	pods, err := p.podInf.GetIndexer().ByIndex(nodeIndex, nodeName)
	if err != nil {
		level.Error(p.logger).Log("msg", "Error getting pods for node", "node", nodeName, "err", err)
		return
	}

	for _, pod := range pods {
		p.enqueue(pod.(*apiv1.Pod))
	}
}

func podSource(pod *apiv1.Pod) string {
	return podSourceFromNamespaceAndName(pod.Namespace, pod.Name)
}

func podSourceFromNamespaceAndName(namespace, name string) string {
	return "pod/" + namespace + "/" + name
}

func podReady(pod *apiv1.Pod) model.LabelValue {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == apiv1.PodReady {
			return lv(strings.ToLower(string(cond.Status)))
		}
	}
	return lv(strings.ToLower(string(apiv1.ConditionUnknown)))
}
