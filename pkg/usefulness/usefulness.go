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

// Package usefulness is the backup quality gate.
//
// A restic/CSI/Velero job that Completed with 12 KiB of TLS certs is not a
// backup. Restore must refuse those artifacts. This package is the shared
// definition of "useful" for Policy.status.backupHealthy and Action preflight.
package usefulness

import (
	"path/filepath"
	"strings"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
	"github.com/PipeOpsHQ/portage/pkg/movers"
)

// MinLogicalBytes is the floor for a real database dump.
// Observed failed Postgres FS backups were ~11.8 KiB (certs/ only).
const MinLogicalBytes int64 = 64 * 1024

// MinVolumeBytes is the floor for a generic filesystem volume tree.
// Larger than MinLogicalBytes so a near-empty mount does not count.
const MinVolumeBytes int64 = 64 * 1024

// File is one path inside a snapshot or dump stream.
type File struct {
	Path      string
	SizeBytes int64
}

// Input is what we know about a candidate artifact.
type Input struct {
	Class     portagev1alpha1.WorkloadClass
	Engine    string
	SizeBytes int64
	Files     []File
	// SnapshotReady is CSI VolumeSnapshot.status.readyToUse.
	SnapshotReady bool
	// HasSnapshot is true when a VolumeSnapshot (or equivalent) exists.
	HasSnapshot bool
}

// Evaluate decides whether an artifact is restorable data, not a successful
// job that captured nothing.
func Evaluate(in Input) movers.Artifact {
	art := movers.Artifact{
		SizeBytes: in.SizeBytes,
	}
	if in.SizeBytes == 0 && len(in.Files) > 0 {
		for _, f := range in.Files {
			art.SizeBytes += f.SizeBytes
		}
	}

	switch in.Class {
	case portagev1alpha1.ClassStateless:
		art.Useful = true
		art.Message = "stateless"
		return art
	case portagev1alpha1.ClassSQLLogical, portagev1alpha1.ClassKVLogical:
		return logical(art, in)
	default:
		return volume(art, in)
	}
}

func logical(art movers.Artifact, in Input) movers.Artifact {
	if dump, ok := bestDump(in.Files); ok {
		if dump.SizeBytes >= MinLogicalBytes {
			art.Useful = true
			art.SizeBytes = dump.SizeBytes
			art.Message = "logical dump " + dump.Path
			return art
		}
		art.Message = "logical dump too small (certs-only class): " + dump.Path
		return art
	}
	if art.SizeBytes >= MinLogicalBytes {
		art.Useful = true
		art.Message = "logical size floor met"
		return art
	}
	if art.SizeBytes > 0 {
		art.Message = "logical backup below size floor (likely empty or certs-only)"
		return art
	}
	if in.HasSnapshot && in.SnapshotReady {
		art.Message = "snapshot ready but no logical dump size; refusing (RequireUseful)"
		return art
	}
	art.Message = "no useful logical dump"
	return art
}

func volume(art movers.Artifact, in Input) movers.Artifact {
	if art.SizeBytes >= MinVolumeBytes {
		art.Useful = true
		art.Message = "volume size floor met"
		return art
	}
	if in.HasSnapshot && in.SnapshotReady && art.SizeBytes == 0 {
		// CSI snapshots do not expose byte size. Treat ReadyToUse as
		// crash-consistent-but-unmeasured — useful for GenericPVC, not for
		// logical engines (handled above).
		if in.Class == portagev1alpha1.ClassGenericPVC || in.Class == portagev1alpha1.ClassUnknownStateful {
			art.Useful = true
			art.Message = "CSI snapshot ReadyToUse (size unknown)"
			return art
		}
	}
	if art.SizeBytes > 0 {
		art.Message = "volume backup below size floor"
		return art
	}
	art.Message = "no useful volume artifact"
	return art
}

func bestDump(files []File) (File, bool) {
	var best File
	found := false
	for _, f := range files {
		if !looksLikeDump(f.Path) {
			continue
		}
		if !found || f.SizeBytes > best.SizeBytes {
			best = f
			found = true
		}
	}
	return best, found
}

func looksLikeDump(path string) bool {
	lower := strings.ToLower(path)
	base := strings.ToLower(filepath.Base(path))
	switch {
	case strings.HasSuffix(base, ".sql"):
		return true
	case strings.HasSuffix(base, ".rdb"):
		return true
	case strings.HasSuffix(base, ".archive.gz"), strings.HasSuffix(base, ".archive"):
		return true
	case strings.HasSuffix(base, ".dump"), strings.HasSuffix(base, ".bak"):
		return true
	case strings.Contains(lower, "prebackup"):
		return true
	default:
		return false
	}
}

// ForWorkload evaluates using class/engine from a classified workload.
func ForWorkload(w classify.Workload, in Input) movers.Artifact {
	in.Class = w.Class
	in.Engine = w.Engine
	return Evaluate(in)
}
