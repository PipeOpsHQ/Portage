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

package version

import "runtime"

var (
	// Version is set via ldflags.
	Version = "dev"
	// GitCommit is set via ldflags.
	GitCommit = "none"
	// BuildTime is set via ldflags.
	BuildTime = "unknown"
)

// Info is build metadata for `portage version`.
func Info() map[string]string {
	return map[string]string{
		"version":   Version,
		"gitCommit": GitCommit,
		"buildTime": BuildTime,
		"goVersion": runtime.Version(),
		"compiler":  runtime.Compiler,
		"platform":  runtime.GOOS + "/" + runtime.GOARCH,
	}
}
