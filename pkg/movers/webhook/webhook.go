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

// Package webhook is an HTTP Mover that forwards Backup/Restore/Replicate
// to an out-of-tree plugin (Velero, restic/K8up, …). The Plugin CR is the
// hot-swap handle: change webhookURL or Policy.mover without rebuilding.
package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/movers"
)

// Mover POSTs JSON to Plugin.spec.webhookURL.
type Mover struct {
	Plugin portagev1alpha1.Plugin
	Client *http.Client
}

func (m Mover) Name() string {
	if m.Plugin.Name != "" {
		return m.Plugin.Name
	}
	return "webhook"
}

func (m Mover) Classes() []portagev1alpha1.WorkloadClass {
	if len(m.Plugin.Spec.Classes) > 0 {
		return m.Plugin.Spec.Classes
	}
	return []portagev1alpha1.WorkloadClass{
		portagev1alpha1.ClassGenericPVC,
		portagev1alpha1.ClassSQLLogical,
		portagev1alpha1.ClassKVLogical,
		portagev1alpha1.ClassSearchFS,
		portagev1alpha1.ClassQueueDurable,
		portagev1alpha1.ClassObjectStore,
		portagev1alpha1.ClassUnknownStateful,
		portagev1alpha1.ClassOperatorManaged,
	}
}

func (m Mover) Discover(_ context.Context, _ classify.Workload) (movers.Capability, error) {
	cap := movers.Capability{
		Backup:    m.Plugin.Spec.Backup,
		Restore:   m.Plugin.Spec.Restore,
		Replicate: m.Plugin.Spec.Replicate,
	}
	if !cap.Backup && !cap.Restore && !cap.Replicate {
		cap.Backup, cap.Restore = true, true
	}
	return cap, nil
}

type request struct {
	Operation string               `json:"operation"`
	Plugin    string               `json:"plugin"`
	Workload  workloadJSON         `json:"workload"`
	Source    movers.ClusterHandle `json:"source"`
	Dest      movers.ClusterHandle `json:"dest"`
	Artifact  *movers.Artifact     `json:"artifact,omitempty"`
}

type workloadJSON struct {
	Namespace string                        `json:"namespace"`
	Name      string                        `json:"name"`
	Kind      string                        `json:"kind"`
	Class     portagev1alpha1.WorkloadClass `json:"class"`
	Engine    string                        `json:"engine,omitempty"`
	PVCNames  []string                      `json:"pvcNames,omitempty"`
}

type response struct {
	Artifact *movers.Artifact    `json:"artifact,omitempty"`
	Probe    *movers.ProbeResult `json:"probe,omitempty"`
	Error    string              `json:"error,omitempty"`
}

func (m Mover) Backup(ctx context.Context, w classify.Workload, dest movers.ClusterHandle) (movers.Artifact, error) {
	res, err := m.call(ctx, "backup", w, movers.ClusterHandle{}, dest, nil)
	if err != nil {
		return movers.Artifact{Mover: m.Name()}, err
	}
	if res.Artifact != nil {
		if res.Artifact.Mover == "" {
			res.Artifact.Mover = m.Name()
		}
		return *res.Artifact, nil
	}
	return movers.Artifact{Mover: m.Name(), Useful: true}, nil
}

func (m Mover) Replicate(ctx context.Context, w classify.Workload, src, dst movers.ClusterHandle) error {
	_, err := m.call(ctx, "replicate", w, src, dst, nil)
	return err
}

func (m Mover) Restore(ctx context.Context, w classify.Workload, artifact movers.Artifact, dst movers.ClusterHandle) error {
	_, err := m.call(ctx, "restore", w, movers.ClusterHandle{}, dst, &artifact)
	return err
}

func (m Mover) Quiesce(ctx context.Context, w classify.Workload) error {
	_, err := m.call(ctx, "quiesce", w, movers.ClusterHandle{}, movers.ClusterHandle{}, nil)
	return err
}

func (m Mover) Promote(ctx context.Context, w classify.Workload, dst movers.ClusterHandle) error {
	_, err := m.call(ctx, "promote", w, movers.ClusterHandle{}, dst, nil)
	return err
}

func (m Mover) Probe(ctx context.Context, w classify.Workload, dst movers.ClusterHandle) (movers.ProbeResult, error) {
	res, err := m.call(ctx, "probe", w, movers.ClusterHandle{}, dst, nil)
	if err != nil {
		return movers.ProbeResult{OK: false, Message: err.Error()}, err
	}
	if res.Probe != nil {
		return *res.Probe, nil
	}
	return movers.ProbeResult{OK: true, Message: "plugin probe"}, nil
}

func (m Mover) call(ctx context.Context, op string, w classify.Workload, src, dst movers.ClusterHandle, art *movers.Artifact) (response, error) {
	url := m.Plugin.Spec.WebhookURL
	if url == "" {
		return response{}, fmt.Errorf("plugin %s: empty webhookURL", m.Name())
	}
	body, err := json.Marshal(request{
		Operation: op,
		Plugin:    m.Name(),
		Workload: workloadJSON{
			Namespace: w.Namespace, Name: w.Name, Kind: w.Kind,
			Class: w.Class, Engine: w.Engine, PVCNames: w.PVCNames,
		},
		Source: src, Dest: dst, Artifact: art,
	})
	if err != nil {
		return response{}, err
	}
	c := m.Client
	if c == nil {
		sec := m.Plugin.Spec.TimeoutSeconds
		if sec <= 0 {
			sec = 120
		}
		c = &http.Client{Timeout: time.Duration(sec) * time.Second}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return response{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpRes, err := c.Do(httpReq)
	if err != nil {
		return response{}, fmt.Errorf("plugin %s %s: %w", m.Name(), op, err)
	}
	defer httpRes.Body.Close()
	if httpRes.StatusCode < 200 || httpRes.StatusCode > 299 {
		return response{}, fmt.Errorf("plugin %s %s: unexpected status %s", m.Name(), op, httpRes.Status)
	}
	var decoded response
	if err := json.NewDecoder(httpRes.Body).Decode(&decoded); err != nil {
		return response{}, fmt.Errorf("plugin %s %s: decode: %w", m.Name(), op, err)
	}
	if decoded.Error != "" {
		return decoded, fmt.Errorf("plugin %s %s: %s", m.Name(), op, decoded.Error)
	}
	return decoded, nil
}

var _ movers.Mover = Mover{}
