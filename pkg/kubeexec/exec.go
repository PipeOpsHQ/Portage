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

// Package kubeexec runs commands in pods.
package kubeexec

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// Result is stdout/stderr plus a combined error (non-zero exit).
type Result struct {
	Stdout string
	Stderr string
}

// Interface execs a command in a container.
type Interface interface {
	Exec(ctx context.Context, namespace, pod, container string, command []string) (Result, error)
}

// SPDY implements Interface with the Kubernetes exec subresource.
type SPDY struct {
	Config *rest.Config
	Client kubernetes.Interface
}

// Exec implements Interface.
func (s SPDY) Exec(ctx context.Context, namespace, pod, container string, command []string) (Result, error) {
	if s.Client == nil || s.Config == nil {
		return Result{}, fmt.Errorf("kubeexec: client/config not configured")
	}
	req := s.Client.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(s.Config, "POST", req.URL())
	if err != nil {
		return Result{}, err
	}
	var stdout, stderr bytes.Buffer
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	}); err != nil {
		return Result{Stdout: stdout.String(), Stderr: stderr.String()}, fmt.Errorf("exec %s/%s: %w", namespace, pod, err)
	}
	return Result{Stdout: stdout.String(), Stderr: stderr.String()}, nil
}

// Fake is an in-memory execer for tests. Key: namespace/pod.
type Fake struct {
	// Results maps namespace/pod to a canned result.
	Results map[string]Result
	// Errors maps namespace/pod to an error.
	Errors map[string]error
	// Calls records Exec invocations.
	Calls []string
}

// Exec implements Interface.
func (f *Fake) Exec(_ context.Context, namespace, pod, container string, command []string) (Result, error) {
	key := namespace + "/" + pod
	f.Calls = append(f.Calls, key+" "+strings.Join(command, " "))
	if f.Errors != nil {
		if err, ok := f.Errors[key]; ok {
			return Result{}, err
		}
	}
	if f.Results != nil {
		if r, ok := f.Results[key]; ok {
			return r, nil
		}
	}
	return Result{}, fmt.Errorf("kubeexec fake: no result for %s (container %s)", key, container)
}
