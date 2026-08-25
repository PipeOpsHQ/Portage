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
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
	"github.com/PipeOpsHQ/portage/pkg/classify"
)

func newInventoryCmd(kf *kubeFlags) *cobra.Command {
	var (
		allNamespaces bool
		output        string
	)
	cmd := &cobra.Command{
		Use:   "inventory",
		Short: "Classify workloads in a namespace (stateless, PVC, engines, unknown)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cc, err := kf.restConfig()
			if err != nil {
				return err
			}
			rest, err := cc.ClientConfig()
			if err != nil {
				return fmt.Errorf("kubeconfig: %w", err)
			}
			ns := kf.namespace
			if ns == "" && !allNamespaces {
				ns, _, err = cc.Namespace()
				if err != nil {
					return err
				}
			}
			cs, err := kubernetes.NewForConfig(rest)
			if err != nil {
				return err
			}
			var nss []string
			if allNamespaces {
				list, err := cs.CoreV1().Namespaces().List(cmd.Context(), metav1.ListOptions{})
				if err != nil {
					return err
				}
				for i := range list.Items {
					nss = append(nss, list.Items[i].Name)
				}
			} else {
				nss = classify.Namespaces(nil, ns)
			}
			inv, err := classify.Walk(cmd.Context(), cs, nss)
			if err != nil {
				return err
			}
			return printInventory(cmd.OutOrStdout(), inv, output)
		},
	}
	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", false, "inventory every namespace")
	cmd.Flags().StringVarP(&output, "output", "o", "table", "table|json")
	return cmd
}

func printInventory(w io.Writer, inv classify.Inventory, format string) error {
	switch strings.ToLower(format) {
	case "json":
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(inv)
	case "table", "":
		tw := tabwriter.NewWriter(w, 0, 8, 2, ' ', 0)
		fmt.Fprintln(tw, "NAMESPACE\tKIND\tNAME\tCLASS\tENGINE\tPVCS")
		for _, wl := range inv.Workloads {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
				wl.Namespace, wl.Kind, wl.Name, wl.Class, wl.Engine, strings.Join(wl.PVCNames, ","))
		}
		if err := tw.Flush(); err != nil {
			return err
		}
		counts := inv.Counts()
		fmt.Fprintf(w, "\n# %d workloads  stateful=%d  unclassified=%d\n",
			len(inv.Workloads), len(inv.Stateful()), counts[portagev1alpha1.ClassUnknownStateful])
		return nil
	default:
		return fmt.Errorf("unknown output %q (table|json)", format)
	}
}
