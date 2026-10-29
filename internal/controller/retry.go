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

package controller

import (
	"context"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// statusConflictRetry is the delay after a reconcile error that must not
// be returned. controller-runtime ignores RequeueAfter when Reconcile
// returns an error, and the failure limiter then stops scheduling that
// object. A conflict is an ordinary concurrent write. The next pass has
// to run.
const statusConflictRetry = 10 * time.Second

func retryLater(ctx context.Context, err error) (ctrl.Result, error) {
	if err == nil {
		return ctrl.Result{}, nil
	}
	log.FromContext(ctx).Error(err, "reconcile failed; retrying")
	return ctrl.Result{RequeueAfter: statusConflictRetry}, nil
}
