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

// Package semver provide the semantic versioning information on CockroachDB.
package semver

import (
	"fmt"
	"regexp"

	"golang.org/x/mod/semver"
)

// For example:
//
//	CockroachDB CCL v23.1.17 (aarch64-apple-darwin21.2, ....)
//	CockroachDB CCL v24.1.0-alpha.5-dev-d45a65e08d45383aade2bcffdcdbe72a0cc279b1 (....)
var verPattern = regexp.MustCompile(`^CockroachDB.* (v\d+\.\d+.\d+(-[^ ]+)?) `)

// CockroachVersion holds the semantic version of a CockroachDB cluster.
type CockroachVersion struct {
	version string
}

// ParseCockroachVersion extracts the semantic version string from a cluster's
// reported version.
func ParseCockroachVersion(version string) (*CockroachVersion, error) {
	found := verPattern.FindStringSubmatch(version)
	if found == nil {
		return nil, fmt.Errorf("could not extract semver from %q", version)
	}
	if !semver.IsValid(found[1]) {
		return nil, fmt.Errorf("not a semver: %q", found[1])
	}
	return &CockroachVersion{version: found[1]}, nil
}

// MustSemver panics if the version string is not a valid semantic version.
func MustSemver(version string) *CockroachVersion {
	if !semver.IsValid(version) {
		panic("invalid version: " + version)
	}
	return &CockroachVersion{version: version}
}

// MinVersion returns true if the CockroachDB version string is
// at least the specified minimum.
func (v *CockroachVersion) MinVersion(minVersion *CockroachVersion) bool {
	return semver.Compare(v.version, minVersion.version) >= 0
}

// String implements the Stringer interface.
func (v *CockroachVersion) String() string {
	return v.version
}
