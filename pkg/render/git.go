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

package render

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/transform"
)

// Git loads dest manifests from a Git tree (or a local directory for tests).
type Git struct {
	Source  *portagev1alpha1.GitSource
	Options transform.Options
	// LookPath overrides exec.LookPath / git binary (tests).
	GitBin string
}

func (Git) Kind() portagev1alpha1.RendererKind { return portagev1alpha1.RendererGit }

func (g Git) Render(ctx context.Context, req Request) ([]*unstructured.Unstructured, error) {
	src := g.Source
	if src == nil && req.Policy != nil {
		src = req.Policy.Spec.Renderer.Git
	}
	if src == nil || src.URL == "" {
		return nil, fmt.Errorf("git renderer: spec.renderer.git.url is required")
	}
	root, cleanup, err := g.checkout(ctx, src)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return nil, err
	}
	dir := root
	if src.Path != "" {
		dir = filepath.Join(root, src.Path)
	}
	objs, err := loadYAMLTree(dir)
	if err != nil {
		return nil, err
	}
	opt := g.Options
	if req.Pair != nil && opt.StorageClassMap == nil {
		opt.StorageClassMap = req.Pair.Spec.StorageClassMap
	}
	for _, o := range objs {
		transform.Object(o, opt)
	}
	return objs, nil
}

func (g Git) checkout(ctx context.Context, src *portagev1alpha1.GitSource) (string, func(), error) {
	url := strings.TrimPrefix(src.URL, "file://")
	if st, err := os.Stat(url); err == nil && st.IsDir() {
		return url, func() {}, nil
	}
	bin := g.GitBin
	if bin == "" {
		bin = "git"
	}
	tmp, err := os.MkdirTemp("", "portage-git-*")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(tmp) }
	args := []string{"clone", "--depth", "1"}
	if src.Ref != "" {
		args = append(args, "--branch", src.Ref)
	}
	args = append(args, src.URL, tmp)
	cmd := exec.CommandContext(ctx, bin, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("git clone: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return tmp, cleanup, nil
}

func loadYAMLTree(root string) ([]*unstructured.Unstructured, error) {
	st, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("git renderer: %w", err)
	}
	var files []string
	if !st.IsDir() {
		files = []string{root}
	} else {
		err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			switch strings.ToLower(filepath.Ext(path)) {
			case ".yaml", ".yml":
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	var out []*unstructured.Unstructured
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		dec := yaml.NewYAMLOrJSONDecoder(strings.NewReader(string(raw)), 4096)
		for {
			obj := &unstructured.Unstructured{}
			if err := dec.Decode(obj); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, fmt.Errorf("%s: %w", f, err)
			}
			if obj.GetKind() == "" {
				continue
			}
			out = append(out, obj)
		}
	}
	return out, nil
}
