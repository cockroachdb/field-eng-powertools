// Copyright 2025 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package semver

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSemver(t *testing.T) {

	tests := []struct {
		name    string
		version string
		want    *CockroachVersion
		wantErr bool
	}{
		{
			name:    "valid version",
			version: "CockroachDB CCL v24.3.17 (aarch64-unknown-linux-gnu, built 2025/07/31 21:20:24, go1.22.8 X:nocoverageredesign)",
			want:    &CockroachVersion{version: "v24.3.17"},
		},
		{
			name:    "valid pre-release",
			version: "CockroachDB CCL v24.3.17-alpha.1 (aarch64-unknown-linux-gnu, built 2025/07/31 21:20:24, go1.22.8 X:nocoverageredesign)",
			want:    &CockroachVersion{version: "v24.3.17-alpha.1"},
		},
		{
			name:    "invalid version",
			version: "CockroachDB CCL (aarch64-unknown-linux-gnu, built 2025/07/31 21:20:24, go1.22.8 X:nocoverageredesign)",
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			got, err := ParseCockroachVersion(tt.version)
			if tt.wantErr {
				a.Error(err)
				return
			}
			a.NoError(err)
			a.Equal(got, tt.want)

		})
	}
}

func TestMinVersion(t *testing.T) {
	tests := []struct {
		name       string
		version    string
		minVersion *CockroachVersion
		want       bool
	}{
		{
			name:       "equal versions",
			version:    "v24.3.17",
			minVersion: &CockroachVersion{version: "v24.3.17"},
			want:       true,
		},
		{
			name:       "greater version",
			version:    "v24.3.18",
			minVersion: &CockroachVersion{version: "v24.3.17"},
			want:       true,
		},
		{
			name:       "lesser version",
			version:    "v24.3.16",
			minVersion: &CockroachVersion{version: "v24.3.17"},
			want:       false,
		},
		{
			name:       "pre-release less than release",
			version:    "v24.3.17-alpha.1",
			minVersion: &CockroachVersion{version: "v24.3.17"},
			want:       false,
		},
		{
			name:       "release greater than pre-release",
			version:    "v24.3.17",
			minVersion: &CockroachVersion{version: "v24.3.17-alpha.1"},
			want:       true,
		},
		{
			name:       "major version greater",
			version:    "v25.0.0",
			minVersion: &CockroachVersion{version: "v24.3.17"},
			want:       true,
		},
		{
			name:       "major version lesser",
			version:    "v23.1.0",
			minVersion: &CockroachVersion{version: "v24.3.17"},
			want:       false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &CockroachVersion{
				version: tt.version,
			}
			if got := v.MinVersion(tt.minVersion); got != tt.want {
				t.Errorf("CockroachVersion.MinVersion() = %v, want %v", got, tt.want)
			}
		})
	}
}
