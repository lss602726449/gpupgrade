// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package greenplum

import (
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/blang/semver/v4"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/testutils/exectest"
)

type DatabaseType int

const (
	Greenplum  DatabaseType = iota // 0
	Cloudberry                     // 1
)

type DatabaseVersion struct {
	Databasetype DatabaseType
	Version      semver.Version
}

func (dv DatabaseVersion) String() string {
	var dbTypeName string
	switch dv.Databasetype {
	case Greenplum:
		dbTypeName = "Greenplum"
	case Cloudberry:
		dbTypeName = "Cloudberry"
	default:
		dbTypeName = "Unknown"
	}
	return fmt.Sprintf("%s %s", dbTypeName, dv.Version.String())
}

func ParseDatabaseVersion(s string) (DatabaseVersion, error) {
	s = strings.TrimSpace(s)
	parts := strings.SplitN(s, " ", 2)
	if len(parts) != 2 {
		return DatabaseVersion{}, fmt.Errorf("invalid database version format: %q, expected 'Type Version'", s)
	}
	var dv DatabaseVersion

	typeStr := strings.TrimSpace(parts[0])
	switch strings.ToLower(typeStr) {
	case "Greenplum", "greenplum":
		dv.Databasetype = Greenplum
	case "Cloudberry", "cloudberry":
		dv.Databasetype = Cloudberry
	default:
		return DatabaseVersion{}, fmt.Errorf("unknown database type: %s", typeStr)
	}

	versionStr := strings.TrimSpace(parts[1])
	version, err := semver.ParseTolerant(versionStr)
	if err != nil {
		return DatabaseVersion{}, fmt.Errorf("invalid version format %q: %w", versionStr, err)
	}

	dv.Version = version
	return dv, nil
}

var versionCommand = exec.Command

// XXX: for internal testing only
func SetVersionCommand(command exectest.Command) {
	versionCommand = command
}

// XXX: for internal testing only
func ResetVersionCommand() {
	versionCommand = exec.Command
}

func Version(gphome string) (DatabaseVersion, error) {
	cmd := versionCommand(filepath.Join(gphome, "bin", "postgres"), "--gp-version")
	cmd.Env = []string{}
	var databasetype DatabaseType
	var parts []string

	log.Printf("Executing: %q", cmd.String())
	output, err := cmd.CombinedOutput()
	if err != nil {
		return DatabaseVersion{-1, semver.Version{}}, fmt.Errorf("%q failed with %q: %w", cmd.String(), string(output), err)
	}

	rawVersion := string(output)
	if strings.Contains(strings.TrimSpace(rawVersion), "postgres (Greenplum Database)") {
		parts = strings.SplitN(strings.TrimSpace(rawVersion), "postgres (Greenplum Database) ", 2)
		databasetype = Greenplum
	} else if strings.Contains(strings.TrimSpace(rawVersion), "postgres (Apache Cloudberry)") {
		parts = strings.SplitN(strings.TrimSpace(rawVersion), "postgres (Apache Cloudberry) ", 2)
		databasetype = Cloudberry
	} else if strings.Contains(strings.TrimSpace(rawVersion), "postgres (Cloudberry Database)") {
		parts = strings.SplitN(strings.TrimSpace(rawVersion), "postgres (Cloudberry Database) ", 2)
		databasetype = Cloudberry
	} else {
		return DatabaseVersion{-1, semver.Version{}}, xerrors.Errorf(`version %q is not of the form "postgres (Greenplum/Cloudberry Database) #.#.#"`, rawVersion)
	}

	pattern := regexp.MustCompile(`\d+\.\d+\.\d+`)
	matches := pattern.FindStringSubmatch(parts[1])
	if len(matches) < 1 {
		return DatabaseVersion{-1, semver.Version{}}, xerrors.Errorf("parsing Greenplum version %q: %w", rawVersion, err)
	}

	version, err := semver.Parse(matches[0])
	if err != nil {
		return DatabaseVersion{-1, semver.Version{}}, xerrors.Errorf("parsing Greenplum version %q: %w", rawVersion, err)
	}

	return DatabaseVersion{databasetype, version}, nil
}
