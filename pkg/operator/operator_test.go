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

package operator

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	monitoringv1 "github.com/GoogleCloudPlatform/prometheus-engine/pkg/operator/apis/monitoring/v1"
)

func TestCleanupOldResources(t *testing.T) {
	var cases = []struct {
		desc             string
		cleanupAnnotKey  string
		collectorAnnots  map[string]string
		evaluatorAnnots  map[string]string
		collectorDeleted bool
		evaluatorDeleted bool
	}{
		{
			desc:            "keep both",
			cleanupAnnotKey: "dont-cleanme",
			collectorAnnots: map[string]string{
				"dont-cleanme": "true",
			},
			evaluatorAnnots: map[string]string{
				"dont-cleanme": "true",
			},
			collectorDeleted: false,
			evaluatorDeleted: false,
		},
		{
			desc:            "delete both",
			cleanupAnnotKey: "dont-cleanme",
			collectorAnnots: map[string]string{
				"cleanme": "true",
			},
			evaluatorAnnots: map[string]string{
				"cleanme": "true",
			},
			collectorDeleted: true,
			evaluatorDeleted: true,
		},
		{
			desc:            "delete collector",
			cleanupAnnotKey: "dont-cleanme",
			collectorAnnots: map[string]string{
				"cleanme": "true",
			},
			evaluatorAnnots: map[string]string{
				"dont-cleanme": "true",
			},
			collectorDeleted: true,
			evaluatorDeleted: false,
		},
		{
			desc:            "delete rule-evaluator",
			cleanupAnnotKey: "dont-cleanme",
			collectorAnnots: map[string]string{
				"dont-cleanme": "true",
			},
			evaluatorAnnots: map[string]string{
				"cleanme": "true",
			},
			collectorDeleted: false,
			evaluatorDeleted: true,
		},
		{
			desc:            "keep both",
			cleanupAnnotKey: "",
			collectorAnnots: map[string]string{
				"dont-cleanme": "true",
			},
			evaluatorAnnots: map[string]string{
				"cleanme": "true",
			},
			collectorDeleted: false,
			evaluatorDeleted: false,
		},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			ds := &appsv1.DaemonSet{
				ObjectMeta: v1.ObjectMeta{
					Name:        NameCollector,
					Namespace:   "gmp-system",
					Annotations: c.collectorAnnots,
				},
			}

			deploy := &appsv1.Deployment{
				ObjectMeta: v1.ObjectMeta{
					Name:        NameRuleEvaluator,
					Namespace:   "gmp-system",
					Annotations: c.evaluatorAnnots,
				},
			}
			opts := Options{
				ProjectID:         "test-proj",
				Location:          "test-loc",
				Cluster:           "test-cluster",
				OperatorNamespace: "gmp-system",
				CleanupAnnotKey:   c.cleanupAnnotKey,
			}
			cl := fake.NewClientBuilder().WithObjects(ds, deploy).Build()

			op := &Operator{
				logger: testr.New(t),
				opts:   opts,
				client: cl,
			}
			if err := op.cleanupOldResources(t.Context()); err != nil {
				t.Fatal(err)
			}

			// Check if collector DaemonSet was preserved.
			var gotDS appsv1.DaemonSet
			dsErr := cl.Get(t.Context(), client.ObjectKey{
				Name:      NameCollector,
				Namespace: "gmp-system",
			}, &gotDS)
			if c.collectorDeleted {
				if !apierrors.IsNotFound(dsErr) {
					t.Errorf("collector should be deleted but found: %+v", gotDS)
				}
			} else if gotDS.Name != ds.Name || gotDS.Namespace != ds.Namespace {
				t.Error("collector DaemonSet differs")
			}

			// Check if rule-evaluator Deployment was preserved.
			var gotDeploy appsv1.Deployment
			deployErr := cl.Get(t.Context(), client.ObjectKey{
				Name:      NameRuleEvaluator,
				Namespace: "gmp-system",
			}, &gotDeploy)
			if c.evaluatorDeleted {
				if !apierrors.IsNotFound(deployErr) {
					t.Errorf("rule-evaluator should be deleted but found: %+v", gotDeploy)
				}
			} else if gotDeploy.Name != deploy.Name || gotDeploy.Namespace != deploy.Namespace {
				t.Error("rule-evaluator Deployment differs")
			}
		})
	}
}

func freeLocalAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on ephemeral port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close ephemeral listener: %v", err)
	}
	return addr
}

func TestHealthServerStartsWhileAPIServerUnresponsive(t *testing.T) {
	unblockAPIServer := make(chan struct{})
	apiserver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-unblockAPIServer
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(func() {
		close(unblockAPIServer)
		apiserver.Close()
	})

	probeAddr := freeLocalAddr(t)
	webhookAddr := freeLocalAddr(t)

	op, err := New(testr.New(t), &rest.Config{Host: apiserver.URL}, Options{
		ProjectID:         "test-proj",
		Location:          "test-loc",
		Cluster:           "test-cluster",
		OperatorNamespace: DefaultOperatorNamespace,
		PublicNamespace:   DefaultPublicNamespace,
		ProbeAddr:         probeAddr,
		ListenAddr:        webhookAddr,
		CertDir:           t.TempDir(),
	})
	if err != nil {
		t.Fatalf("New() failed while kube-apiserver was unresponsive: %v", err)
	}

	runCtx, cancelRun := context.WithCancel(t.Context())
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- op.Run(runCtx, prometheus.NewRegistry())
	}()
	t.Cleanup(func() {
		cancelRun()
		<-runErrCh
	})

	httpClient := &http.Client{Timeout: 500 * time.Millisecond}
	for _, endpoint := range []string{"/healthz", "/readyz"} {
		url := fmt.Sprintf("http://%s%s", probeAddr, endpoint)
		err := wait.PollUntilContextTimeout(t.Context(), 50*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				return false, err
			}
			resp, err := httpClient.Do(req)
			if err != nil {
				return false, nil
			}
			defer resp.Body.Close()
			return resp.StatusCode == http.StatusOK, nil
		})
		if err != nil {
			t.Fatalf("probe %s did not become healthy while kube-apiserver was unresponsive: %v", endpoint, err)
		}
	}
}

func TestOperatorConfigValidatorVPAAvailability(t *testing.T) {
	oc := &monitoringv1.OperatorConfig{
		ObjectMeta: v1.ObjectMeta{
			Namespace: DefaultPublicNamespace,
			Name:      NameOperatorConfig,
		},
		Scaling: monitoringv1.ScalingSpec{
			VPA: monitoringv1.VPASpec{
				Enabled: true,
			},
		},
	}

	op, err := New(testr.New(t), &rest.Config{Host: "http://127.0.0.1:1"}, Options{
		ProjectID:         "test-proj",
		Location:          "test-loc",
		Cluster:           "test-cluster",
		OperatorNamespace: DefaultOperatorNamespace,
		PublicNamespace:   DefaultPublicNamespace,
		ProbeAddr:         "0",
		ListenAddr:        freeLocalAddr(t),
		CertDir:           t.TempDir(),
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	op.clientset = apiextensionsfake.NewClientset()

	validator := &operatorConfigValidator{
		namespace:    DefaultPublicNamespace,
		name:         NameOperatorConfig,
		vpaAvailable: &op.vpaAvailable,
	}

	if err := op.setupVPA(t.Context()); err != nil {
		t.Fatalf("setupVPA without CRD failed: %v", err)
	}
	if op.vpaAvailable.Load() {
		t.Fatal("expected vpaAvailable to be false when CRD is not installed")
	}
	if _, err := validator.ValidateCreate(t.Context(), oc); err == nil {
		t.Fatal("expected ValidateCreate to reject VPA-enabled OperatorConfig when VPA is unavailable")
	}

	vpaCRD := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: v1.ObjectMeta{
			Name: "verticalpodautoscalers.autoscaling.k8s.io",
		},
	}
	op.clientset = apiextensionsfake.NewClientset(vpaCRD)
	if err := op.setupVPA(t.Context()); err != nil {
		t.Fatalf("setupVPA with CRD failed: %v", err)
	}
	if !op.vpaAvailable.Load() {
		t.Fatal("expected vpaAvailable to be true when CRD is installed")
	}
	if _, err := validator.ValidateCreate(t.Context(), oc); err != nil {
		t.Fatalf("expected ValidateCreate to allow VPA-enabled OperatorConfig when VPA is available, got: %v", err)
	}
}
