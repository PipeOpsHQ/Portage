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

package postgres

import "testing"

func TestParseReplayLag(t *testing.T) {
	t.Parallel()
	cases := []struct {
		out     string
		lag     int64
		standby bool
		err     bool
	}{
		{"0\n", 0, true, false},
		{"8192", 8192, true, false},
		// A primary (restored dest with a live pod) is not "lag 0".
		{"-1", 0, false, false},
		{"-2", 0, true, true},
		// psql printed nothing / an error: not zero lag.
		{"", 0, false, true},
		{"psql: error", 0, false, true},
	}
	for _, c := range cases {
		lag, standby, err := parseReplayLag(c.out)
		if lag != c.lag || standby != c.standby || (err != nil) != c.err {
			t.Errorf("%q: lag=%d standby=%v err=%v", c.out, lag, standby, err)
		}
	}
}
