// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package operator contains the Prometheus operator.
package operator

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/client_golang/prometheus"
	arv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeutil "k8s.io/apimachinery/pkg/util/runtime"
	autoscalingv1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	monitoringv1 "github.com/GoogleCloudPlatform/prometheus-engine/pkg/operator/apis/monitoring/v1"
)

const (
	// DefaultOperatorNamespace is the namespace in which all resources owned by the operator are installed.
	DefaultOperatorNamespace = "gmp-system"
	// DefaultPublicNamespace is the namespace where the operator will check for user-specified
	// configuration data.
	DefaultPublicNamespace = "gmp-public"

	// NameOperator is a fixed name used in various resources managed by the operator.
	NameOperator = "gmp-operator"
	// componentName is a fixed name used in various resources managed by the operator.
	componentName = "managed_prometheus"

	// Filename for configuration files.
	configFilename = "config.yaml"

	// LabelAppName is the well-known app name label.
	LabelAppName = "app.kubernetes.io/name"
	// LabelInstanceName is the well-known instance name label.
	LabelInstanceName = "app.kubernetes.io/instance"

	// AnnotationMetricName is the component name, will be exposed as metric name.
	AnnotationMetricName = "components.gke.io/component-name"
	// ClusterAutoscalerSafeEvictionLabel is the annotation label that determines
	// whether the cluster autoscaler can safely evict a Pod when the Pod doesn't
	// satisfy certain eviction criteria.
	ClusterAutoscalerSafeEvictionLabel = "cluster-autoscaler.kubernetes.io/safe-to-evict"

	// KubernetesAppName is the k8s Application, will be exposed as component name.
	KubernetesAppName = "app"
	// RuleEvaluatorAppName is the name of the rule-evaluator application.
	RuleEvaluatorAppName = "managed-prometheus-rule-evaluator"
	// AlertmanagerAppName is the name of the alert manager application.
	AlertmanagerAppName = "managed-prometheus-alertmanager"

	// The level of concurrency to use to fetch all targets.
	defaultTargetPollConcurrency = 4
)

// Operator to implement managed collection for Google Prometheus Engine.
type Operator struct {
	logger       logr.Logger
	opts         Options
	client       client.Client
	clientset    apiextensions.Interface
	manager      manager.Manager
	vpaAvailable atomic.Bool
}

// Options for the Operator.
type Options struct {
	// ID of the project of the cluster.
	ProjectID string
	// Location of the cluster.
	Location string
	// Name of the cluster the operator acts on.
	Cluster string
	// Namespace to which the operator deploys any associated resources.
	OperatorNamespace string
	// Namespace to which the operator looks for user-specified configuration
	// data, like Secrets and ConfigMaps.
	PublicNamespace string
	// Health and readiness serving address.
	ProbeAddr string
	// Certificate of the server in base 64.
	TLSCert string
	// Key of the server in base 64.
	TLSKey string
	// Certificate authority in base 64.
	CACert string
	// CertDir is the path to a directory containing TLS certificates for the webhook server.
	CertDir string
	// Webhook serving address.
	ListenAddr string
	// Cleanup resources without this annotation.
	CleanupAnnotKey string
	// The number of upper bound threads to use for target polling otherwise
	// use the default.
	TargetPollConcurrency uint16
	// The HTTP client to use when targeting collector endpoints.
	CollectorHTTPClient *http.Client
}

func (o *Options) defaultAndValidate(_ logr.Logger) error {
	if o.OperatorNamespace == "" {
		o.OperatorNamespace = DefaultOperatorNamespace
	}
	if o.PublicNamespace == "" {
		// For non-managed deployments, default to same namespace
		// as operator, assuming cluster operators prefer consolidating
		// resources in a single namespace.
		o.PublicNamespace = DefaultOperatorNamespace
	}

	// ProjectID and Cluster must be always be set. Collectors and rule-evaluator can
	// auto-discover them but we need them in the operator to scope generated rules.
	if o.ProjectID == "" {
		return errors.New("projectID must be set")
	}
	if o.Cluster == "" {
		return errors.New("cluster must be set")
	}

	if o.TargetPollConcurrency == 0 {
		o.TargetPollConcurrency = defaultTargetPollConcurrency
	}
	if o.CollectorHTTPClient == nil {
		// Matches the default Prometheus API library HTTP client.
		o.CollectorHTTPClient = &http.Client{
			Transport: api.DefaultRoundTripper,
		}
	}
	return nil
}

// NewScheme creates a new Kubernetes runtime.Scheme for the GMP Operator.
func NewScheme() (*runtime.Scheme, error) {
	sc := runtime.NewScheme()

	if err := scheme.AddToScheme(sc); err != nil {
		return nil, fmt.Errorf("add Kubernetes core scheme: %w", err)
	}
	if err := monitoringv1.AddToScheme(sc); err != nil {
		return nil, fmt.Errorf("add monitoringv1 scheme: %w", err)
	}
	if err := autoscalingv1.AddToScheme(sc); err != nil {
		return nil, fmt.Errorf("add autoscalerv1 scheme: %w", err)
	}
	return sc, nil
}

// newRESTMapper creates a RESTMapper that resolves known operator resource types
// statically in memory before falling back to dynamic API discovery, avoiding
// synchronous kube-apiserver discovery calls during manager initialization.
func newRESTMapper(cfg *rest.Config, httpClient *http.Client) (meta.RESTMapper, error) {
	dynamicMapper, err := apiutil.NewDynamicRESTMapper(cfg, httpClient)
	if err != nil {
		return nil, err
	}

	staticMapper := meta.NewDefaultRESTMapper(nil)
	staticMapper.Add(corev1.SchemeGroupVersion.WithKind("Pod"), meta.RESTScopeNamespace)
	staticMapper.Add(corev1.SchemeGroupVersion.WithKind("Secret"), meta.RESTScopeNamespace)
	staticMapper.Add(corev1.SchemeGroupVersion.WithKind("Service"), meta.RESTScopeNamespace)
	staticMapper.Add(corev1.SchemeGroupVersion.WithKind("ConfigMap"), meta.RESTScopeNamespace)
	staticMapper.Add(appsv1.SchemeGroupVersion.WithKind("DaemonSet"), meta.RESTScopeNamespace)
	staticMapper.Add(appsv1.SchemeGroupVersion.WithKind("Deployment"), meta.RESTScopeNamespace)
	staticMapper.Add(appsv1.SchemeGroupVersion.WithKind("StatefulSet"), meta.RESTScopeNamespace)
	staticMapper.Add(arv1.SchemeGroupVersion.WithKind("ValidatingWebhookConfiguration"), meta.RESTScopeRoot)
	staticMapper.Add(arv1.SchemeGroupVersion.WithKind("MutatingWebhookConfiguration"), meta.RESTScopeRoot)
	staticMapper.Add(autoscalingv1.SchemeGroupVersion.WithKind("VerticalPodAutoscaler"), meta.RESTScopeNamespace)

	staticMapper.Add(monitoringv1.SchemeGroupVersion.WithKind("PodMonitoring"), meta.RESTScopeNamespace)
	staticMapper.Add(monitoringv1.SchemeGroupVersion.WithKind("ClusterPodMonitoring"), meta.RESTScopeRoot)
	staticMapper.Add(monitoringv1.SchemeGroupVersion.WithKind("ClusterNodeMonitoring"), meta.RESTScopeRoot)
	staticMapper.Add(monitoringv1.SchemeGroupVersion.WithKind("OperatorConfig"), meta.RESTScopeNamespace)
	staticMapper.AddSpecific(
		monitoringv1.SchemeGroupVersion.WithKind("Rules"),
		monitoringv1.SchemeGroupVersion.WithResource("rules"),
		monitoringv1.SchemeGroupVersion.WithResource("rules"),
		meta.RESTScopeNamespace,
	)
	staticMapper.AddSpecific(
		monitoringv1.SchemeGroupVersion.WithKind("ClusterRules"),
		monitoringv1.SchemeGroupVersion.WithResource("clusterrules"),
		monitoringv1.SchemeGroupVersion.WithResource("clusterrules"),
		meta.RESTScopeRoot,
	)
	staticMapper.AddSpecific(
		monitoringv1.SchemeGroupVersion.WithKind("GlobalRules"),
		monitoringv1.SchemeGroupVersion.WithResource("globalrules"),
		monitoringv1.SchemeGroupVersion.WithResource("globalrules"),
		meta.RESTScopeRoot,
	)

	return meta.FirstHitRESTMapper{
		MultiRESTMapper: meta.MultiRESTMapper{
			staticMapper,
			dynamicMapper,
		},
	}, nil
}

// New instantiates a new Operator.
func New(logger logr.Logger, clientConfig *rest.Config, opts Options) (*Operator, error) {
	if err := opts.defaultAndValidate(logger); err != nil {
		return nil, fmt.Errorf("invalid options: %w", err)
	}

	sc, err := NewScheme()
	if err != nil {
		return nil, fmt.Errorf("unable to initialize Kubernetes scheme: %w", err)
	}

	host, portStr, err := net.SplitHostPort(opts.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid listen address: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("invalid port: %w", err)
	}

	watchObjects := map[client.Object]cache.ByObject{
		&corev1.Pod{}: {
			Field: fields.SelectorFromSet(fields.Set{"metadata.namespace": opts.OperatorNamespace}),
		},
		&monitoringv1.PodMonitoring{}: {
			Field: fields.Everything(),
		},
		&monitoringv1.ClusterPodMonitoring{}: {
			Field: fields.Everything(),
		},
		&monitoringv1.ClusterNodeMonitoring{}: {
			Field: fields.Everything(),
		},
		&monitoringv1.GlobalRules{}: {
			Field: fields.Everything(),
		},
		&monitoringv1.ClusterRules{}: {
			Field: fields.Everything(),
		},
		&monitoringv1.Rules{}: {
			Field: fields.Everything(),
		},
		&corev1.Secret{}: {
			Namespaces: map[string]cache.Config{
				opts.OperatorNamespace: {},
				opts.PublicNamespace:   {},
			},
		},
		&monitoringv1.OperatorConfig{}: {
			Field: fields.SelectorFromSet(fields.Set{"metadata.namespace": opts.PublicNamespace}),
		},
		&corev1.Service{}: {
			Field: fields.SelectorFromSet(fields.Set{
				"metadata.namespace": opts.OperatorNamespace,
				"metadata.name":      NameAlertmanager,
			}),
		},
		&corev1.ConfigMap{}: {
			Field: fields.SelectorFromSet(fields.Set{"metadata.namespace": opts.OperatorNamespace}),
		},
		&appsv1.DaemonSet{}: {
			Field: fields.SelectorFromSet(fields.Set{
				"metadata.namespace": opts.OperatorNamespace,
				"metadata.name":      NameCollector,
			}),
		},
		&appsv1.Deployment{}: {
			Field: fields.SelectorFromSet(fields.Set{
				"metadata.namespace": opts.OperatorNamespace,
				"metadata.name":      NameRuleEvaluator,
			}),
		},
		&appsv1.StatefulSet{}: {
			Field: fields.SelectorFromSet(fields.Set{
				"metadata.namespace": opts.OperatorNamespace,
				"metadata.name":      NameAlertmanager,
			}),
		},
		&autoscalingv1.VerticalPodAutoscaler{}: {
			Field: fields.SelectorFromSet(fields.Set{
				"metadata.namespace": opts.OperatorNamespace,
			}),
		},
	}

	coreClientConfig := rest.CopyConfig(clientConfig)
	coreClientConfig.ContentType = runtime.ContentTypeProtobuf
	clientset, err := apiextensions.NewForConfig(coreClientConfig)
	if err != nil {
		return nil, fmt.Errorf("create clientset: %w", err)
	}

	manager, err := ctrl.NewManager(clientConfig, manager.Options{
		Logger:         logger,
		Scheme:         sc,
		MapperProvider: newRESTMapper,
		WebhookServer: webhook.NewServer(webhook.Options{
			Host:    host,
			Port:    port,
			CertDir: opts.CertDir,
		}),
		// Don't run a metrics server with the manager. Metrics are being served.
		// explicitly in the main routine.
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
		HealthProbeBindAddress: opts.ProbeAddr,
		// Manage cluster-wide and namespace resources at the same time.
		Cache: cache.Options{
			// The presence of metadata.namespace has special handling internally causing the
			// cache's watch-list to only watch that namespace.
			ByObject: watchObjects,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create controller manager: %w", err)
	}

	webhookChecker := manager.GetWebhookServer().StartedChecker()
	if err := manager.AddHealthzCheck("webhooks", webhookChecker); err != nil {
		return nil, fmt.Errorf("add healthz check for webhooks: %w", err)
	}
	if err := manager.AddReadyzCheck("webhooks", webhookChecker); err != nil {
		return nil, fmt.Errorf("add readyz check for webhooks: %w", err)
	}

	client, err := client.New(clientConfig, client.Options{
		Scheme: sc,
		Mapper: manager.GetRESTMapper(),
	})
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}

	op := &Operator{
		logger:    logger,
		opts:      opts,
		client:    client,
		clientset: clientset,
		manager:   manager,
	}
	return op, nil
}

// Run the reconciliation loop of the operator.
// The passed owner references are set on cluster-wide resources created by the
// operator.
func (o *Operator) Run(ctx context.Context, registry prometheus.Registerer) error {
	defer runtimeutil.HandleCrash()

	if err := setupAdmissionWebhooks(ctx, o.logger, o.client, o.manager.GetWebhookServer().(*webhook.DefaultServer), &o.opts, &o.vpaAvailable); err != nil {
		return fmt.Errorf("init admission resources: %w", err)
	}
	if err := setupCollectionControllers(o); err != nil {
		return fmt.Errorf("setup collection controllers: %w", err)
	}
	if err := setupRulesControllers(o); err != nil {
		return fmt.Errorf("setup rules controllers: %w", err)
	}
	if err := setupOperatorConfigControllers(o); err != nil {
		return fmt.Errorf("setup rule-evaluator controllers: %w", err)
	}
	if err := setupTargetStatusPoller(o, registry, o.opts.CollectorHTTPClient); err != nil {
		return fmt.Errorf("setup target status processor: %w", err)
	}
	if err := o.manager.Add(manager.RunnableFunc(o.cleanupOldResources)); err != nil {
		return fmt.Errorf("add cleanup old resources runnable: %w", err)
	}
	if err := o.manager.Add(manager.RunnableFunc(o.setupVPA)); err != nil {
		return fmt.Errorf("add setup VPA runnable: %w", err)
	}

	o.logger.Info("starting GMP operator")
	return o.manager.Start(ctx)
}

func (o *Operator) setupVPA(ctx context.Context) error {
	// Determine whether VPA is installed in the cluster. If so, set up the scaling controller.
	if o.clientset != nil {
		if _, err := o.clientset.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, "verticalpodautoscalers.autoscaling.k8s.io", metav1.GetOptions{}); err != nil {
			o.logger.Info("vertical pod autoscaling is not available, scaling.vpa.enabled option on the OperatorConfig will not work")
		} else {
			o.logger.Info("vertical pod autoscaling available, monitoring OperatorConfig for scaling.vpa.enabled option")
			o.vpaAvailable.Store(true)
		}
	}
	if o.vpaAvailable.Load() {
		if err := setupScalingController(o); err != nil {
			return fmt.Errorf("setup scaling controllers: %w", err)
		}
	}
	return nil
}

func (o *Operator) cleanupOldResources(ctx context.Context) error {
	// Delete old ValidatingWebhookConfiguration that was installed directly by the operator.
	// in previous versions.
	validatingWebhookConfig := arv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "gmp-operator"},
	}
	if err := o.client.Delete(ctx, &validatingWebhookConfig); err != nil {
		switch {
		case apierrors.IsForbidden(err):
			o.logger.Info("delete legacy ValidatingWebHookConfiguration was not allowed. Please remove it manually")
		case !apierrors.IsNotFound(err):
			return fmt.Errorf("delete legacy ValidatingWebHookConfiguration failed: %w", err)
		default:
			// Noop.
		}
	}

	// If cleanup annotations are not provided, do not clean up any further.
	if o.opts.CleanupAnnotKey == "" {
		return nil
	}

	// Cleanup resources without the provided annotation.
	// Check the collector DaemonSet.
	dsKey := client.ObjectKey{
		Name:      NameCollector,
		Namespace: o.opts.OperatorNamespace,
	}
	var ds appsv1.DaemonSet
	if err := o.client.Get(ctx, dsKey, &ds); apierrors.IsNotFound(err) {
		return fmt.Errorf("get collector DaemonSet: %w", err)
	}
	if _, ok := ds.Annotations[o.opts.CleanupAnnotKey]; !ok {
		if err := o.client.Delete(ctx, &ds); err != nil {
			switch {
			case apierrors.IsForbidden(err):
				o.logger.Info("delete collector was not allowed. Please remove it manually", "err", err)
			case !apierrors.IsNotFound(err):
				return fmt.Errorf("cleanup collector failed: %w", err)
			default:
				// Noop.
			}
		}
	}

	// Check the rule-evaluator Deployment.
	deployKey := client.ObjectKey{
		Name:      NameRuleEvaluator,
		Namespace: o.opts.OperatorNamespace,
	}
	var deploy appsv1.Deployment
	if err := o.client.Get(ctx, deployKey, &deploy); apierrors.IsNotFound(err) {
		return fmt.Errorf("get rule-evaluator Deployment: %w", err)
	}
	if _, ok := deploy.Annotations[o.opts.CleanupAnnotKey]; !ok {
		if err := o.client.Delete(ctx, &deploy); err != nil {
			switch {
			case apierrors.IsForbidden(err):
				o.logger.Info("delete rule-evaluator was not allowed. Please remove it manually", "err", err)
			case !apierrors.IsNotFound(err):
				return fmt.Errorf("cleanup rule-evaluator failed: %w", err)
			default:
				// Noop.
			}
		}
	}

	return nil
}

// namespacedNamePredicate is an event filter predicate that only allows events with
// a single object.
type namespacedNamePredicate struct {
	namespace string
	name      string
}

func (o namespacedNamePredicate) Create(e event.CreateEvent) bool {
	return e.Object.GetNamespace() == o.namespace && e.Object.GetName() == o.name
}

func (o namespacedNamePredicate) Update(e event.UpdateEvent) bool {
	return e.ObjectNew.GetNamespace() == o.namespace && e.ObjectNew.GetName() == o.name
}

func (o namespacedNamePredicate) Delete(e event.DeleteEvent) bool {
	return e.Object.GetNamespace() == o.namespace && e.Object.GetName() == o.name
}

func (o namespacedNamePredicate) Generic(e event.GenericEvent) bool {
	return e.Object.GetNamespace() == o.namespace && e.Object.GetName() == o.name
}

// enqueueConst always enqueues the same request regardless of the event.
type enqueueConst reconcile.Request

func (e enqueueConst) Create(_ context.Context, _ event.TypedCreateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	q.Add(reconcile.Request(e))
}

func (e enqueueConst) Update(_ context.Context, _ event.TypedUpdateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	q.Add(reconcile.Request(e))
}

func (e enqueueConst) Delete(_ context.Context, _ event.TypedDeleteEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	q.Add(reconcile.Request(e))
}

func (e enqueueConst) Generic(_ context.Context, _ event.TypedGenericEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	q.Add(reconcile.Request(e))
}
