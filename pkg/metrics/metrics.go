/*
Copyright 2026 PipeOps and the Portage Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// BackupUseful is 1 when a policy's backups are useful.
	BackupUseful = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "portage",
		Name:      "backup_useful",
		Help:      "1 if Policy backupHealthy is true",
	}, []string{"policy", "namespace"})

	// Actions is a count of Action terminal phases.
	Actions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "portage",
		Name:      "actions_total",
		Help:      "Terminal Actions by type and phase",
	}, []string{"type", "phase"})
)

func init() {
	metrics.Registry.MustRegister(BackupUseful, Actions)
}

// ObservePolicy sets backup_useful.
func ObservePolicy(policy, namespace string, useful bool) {
	v := 0.0
	if useful {
		v = 1
	}
	BackupUseful.WithLabelValues(policy, namespace).Set(v)
}

// ObserveAction increments the terminal counter.
func ObserveAction(typ, phase string) {
	Actions.WithLabelValues(typ, phase).Inc()
}
