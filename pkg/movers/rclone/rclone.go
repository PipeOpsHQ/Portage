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

// Package rclone is the ObjectStore-hop VolSync mover.
// Portage does not ship rclone; it emits a VolSync ReplicationSource with
// spec.rclone pointing at the ClusterPair bucket.
package rclone

import (
	"k8s.io/client-go/dynamic"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/movers/volsync"
)

// New returns a VolSync mover pinned to rclone/object-store transport.
func New(dyn dynamic.Interface, destPath string) volsync.Mover {
	return volsync.Mover{
		Dynamic:     dyn,
		Transport:   portagev1alpha1.TransportObjectStore,
		DestPath:    destPath,
		ObjectMover: "rclone",
	}
}

// Name is the override key for Policy.spec.moverOverrides.
const Name = "rclone"
