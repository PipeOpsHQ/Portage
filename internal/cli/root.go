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

package cli

import (
	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/clientcmd"
)

// NewRoot returns the `portage` command tree.
func NewRoot() *cobra.Command {
	cfg := &kubeFlags{}
	cmd := &cobra.Command{
		Use:           "portage",
		Short:         "Portage by PipeOps — Kubernetes workload mobility (replicate, restore, cutover)",
		Long:          longHelp,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	cmd.PersistentFlags().StringVar(&cfg.kubeconfig, "kubeconfig", "", "path to kubeconfig (default: KUBECONFIG or ~/.kube/config)")
	cmd.PersistentFlags().StringVar(&cfg.context, "context", "", "kubeconfig context")
	cmd.PersistentFlags().StringVarP(&cfg.namespace, "namespace", "n", "", "namespace (default: current context namespace)")

	cmd.AddCommand(newInventoryCmd(cfg))
	cmd.AddCommand(newVersionCmd())
	cmd.AddCommand(newCompletionCmd())
	return cmd
}

const longHelp = `Portage by PipeOps moves stateful Kubernetes workloads between clusters and clouds.

It classifies every workload, keeps a useful copy of its state, optionally
replicates to a warm destination, and will not call restore or cutover complete
until the workload is Ready and a class probe attests the data.

Portage orchestrates existing CNCF data planes (VolSync, CSI snapshots, and
engine-native replicas). It is not a Velero replacement for etcd-adjacent
cluster backup, and it does not invent a new rsync.`

type kubeFlags struct {
	kubeconfig string
	context    string
	namespace  string
}

func (k *kubeFlags) loadingRules() *clientcmd.ClientConfigLoadingRules {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if k.kubeconfig != "" {
		rules.ExplicitPath = k.kubeconfig
	}
	return rules
}

func (k *kubeFlags) overrides() *clientcmd.ConfigOverrides {
	return &clientcmd.ConfigOverrides{CurrentContext: k.context}
}

func (k *kubeFlags) restConfig() (clientcmd.ClientConfig, error) {
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(k.loadingRules(), k.overrides()), nil
}
