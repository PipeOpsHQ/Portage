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

package classify

import (
	"strings"

	portagev1alpha1 "github.com/PipeOpsHQ/portage/api/v1alpha1"
)

// Engine is a known data engine. Order of Catalog matters: first match wins,
// so more specific matchers (mariadb, timescaledb) must come first.
type Engine struct {
	Name          string
	Class         portagev1alpha1.WorkloadClass
	ImageMatchers []string
	// CRDMatchers are Group/Kind strings, e.g. "postgresql.cnpg.io/Cluster".
	CRDMatchers []string
	// LogicalDump is true when a stdout dump exists and is the preferred backup.
	LogicalDump bool
}

// Catalog is the built-in engine list. Platforms extend this via Register.
var Catalog = []Engine{
	{Name: "timescale", Class: portagev1alpha1.ClassSQLLogical, LogicalDump: true,
		ImageMatchers: []string{"timescale/timescaledb", "timescaledb"}},
	{Name: "cockroach", Class: portagev1alpha1.ClassSQLLogical, LogicalDump: false,
		ImageMatchers: []string{"cockroachdb/cockroach", "cockroachdb"}},
	{Name: "postgres", Class: portagev1alpha1.ClassSQLLogical, LogicalDump: true,
		ImageMatchers: []string{
			"bitnami/postgresql", "cloudnative-pg", "crunchydata", "pgvector",
			"supabase/postgres", "enterprisedb", "postgis", "postgresql", "postgres",
		},
		CRDMatchers: []string{"postgresql.cnpg.io/Cluster", "postgres-operator.crunchydata.com/PostgresCluster"},
	},
	{Name: "mariadb", Class: portagev1alpha1.ClassSQLLogical, LogicalDump: true,
		ImageMatchers: []string{"bitnami/mariadb", "mariadb"}},
	{Name: "mysql", Class: portagev1alpha1.ClassSQLLogical, LogicalDump: true,
		ImageMatchers: []string{"bitnami/mysql", "percona/percona-server", "mysql/mysql-server", "mysql"},
		CRDMatchers:   []string{"pxc.percona.com/PerconaXtraDBCluster", "mysql.presslabs.org/MysqlCluster"},
	},
	{Name: "mssql", Class: portagev1alpha1.ClassSQLLogical, LogicalDump: false,
		ImageMatchers: []string{"mssql/server", "microsoft/mssql-server", "azure-sql-edge"}},
	{Name: "clickhouse", Class: portagev1alpha1.ClassSQLLogical, LogicalDump: true,
		ImageMatchers: []string{"clickhouse/clickhouse-server", "bitnami/clickhouse", "clickhouse"}},
	{Name: "mongo", Class: portagev1alpha1.ClassKVLogical, LogicalDump: true,
		ImageMatchers: []string{"bitnami/mongodb", "percona/percona-server-mongodb", "mongodb/mongodb-community-server", "mongo"},
		CRDMatchers:   []string{"psmdb.percona.com/PerconaServerMongoDB"},
	},
	{Name: "redis", Class: portagev1alpha1.ClassKVLogical, LogicalDump: true,
		ImageMatchers: []string{"bitnami/redis", "redis/redis-stack", "redis-stack", "redis"},
		CRDMatchers:   []string{"redis.redis.opstreelabs.in/Redis", "databases.spotahome.com/RedisFailover"},
	},
	{Name: "valkey", Class: portagev1alpha1.ClassKVLogical, LogicalDump: true,
		ImageMatchers: []string{"valkey/valkey", "bitnami/valkey", "valkey"}},
	{Name: "keydb", Class: portagev1alpha1.ClassKVLogical, LogicalDump: true,
		ImageMatchers: []string{"eqalpha/keydb", "keydb"}},
	{Name: "dragonfly", Class: portagev1alpha1.ClassKVLogical, LogicalDump: true,
		ImageMatchers: []string{"dragonflydb/dragonfly", "dragonfly"}},
	{Name: "elasticsearch", Class: portagev1alpha1.ClassSearchFS, LogicalDump: false,
		ImageMatchers: []string{"bitnami/elasticsearch", "elastic/elasticsearch", "elasticsearch"}},
	{Name: "opensearch", Class: portagev1alpha1.ClassSearchFS, LogicalDump: false,
		ImageMatchers: []string{"opensearchproject/opensearch", "bitnami/opensearch", "opensearch"}},
	{Name: "cassandra", Class: portagev1alpha1.ClassSearchFS, LogicalDump: false,
		ImageMatchers: []string{"bitnami/cassandra", "cassandra"}},
	{Name: "scylla", Class: portagev1alpha1.ClassSearchFS, LogicalDump: false,
		ImageMatchers: []string{"scylladb/scylla", "scylla"}},
	{Name: "neo4j", Class: portagev1alpha1.ClassSearchFS, LogicalDump: false,
		ImageMatchers: []string{"neo4j"}},
	{Name: "influxdb", Class: portagev1alpha1.ClassSearchFS, LogicalDump: false,
		ImageMatchers: []string{"bitnami/influxdb", "influxdb"}},
	{Name: "rabbitmq", Class: portagev1alpha1.ClassQueueDurable, LogicalDump: true,
		ImageMatchers: []string{"bitnami/rabbitmq", "rabbitmq"}},
	{Name: "nats", Class: portagev1alpha1.ClassQueueDurable, LogicalDump: false,
		ImageMatchers: []string{"natsio/nats-server", "bitnami/nats", "nats"}},
	{Name: "kafka", Class: portagev1alpha1.ClassQueueDurable, LogicalDump: false,
		ImageMatchers: []string{"bitnami/kafka", "confluentinc/cp-kafka", "strimzi", "kafka"},
		CRDMatchers:   []string{"kafka.strimzi.io/Kafka"},
	},
	{Name: "minio", Class: portagev1alpha1.ClassObjectStore, LogicalDump: false,
		ImageMatchers: []string{"bitnami/minio", "minio/minio", "minio"}},
	{Name: "etcd", Class: portagev1alpha1.ClassKVLogical, LogicalDump: true,
		ImageMatchers: []string{"bitnami/etcd", "quay.io/coreos/etcd", "etcd"}},
}

// MatchImage returns the first engine whose matcher is a substring of image.
func MatchImage(image string) (Engine, bool) {
	lower := strings.ToLower(image)
	for _, e := range Catalog {
		for _, m := range e.ImageMatchers {
			if m != "" && strings.Contains(lower, strings.ToLower(m)) {
				return e, true
			}
		}
	}
	return Engine{}, false
}

// MatchCRD returns the engine for a Group/Kind, if registered.
func MatchCRD(groupKind string) (Engine, bool) {
	for _, e := range Catalog {
		for _, m := range e.CRDMatchers {
			if strings.EqualFold(m, groupKind) {
				return e, true
			}
		}
	}
	return Engine{}, false
}

// Register appends an engine. Used by out-of-tree catalogs (a PaaS, a vendor).
func Register(e Engine) {
	Catalog = append([]Engine{e}, Catalog...)
}
