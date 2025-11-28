// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package greenplum

import (
	"fmt"

	"github.com/blang/semver/v4"
)

// Change these values to bump the minimum supported versions and associated tests.
const minGreenplum5xVersion = "5.29.10"
const minGreenplum6xVersion = "6.0.0"
const minGreenplum7xVersion = "7.0.0"

var GetSourceVersion = Version
var GetTargetVersion = Version

func VerifyCompatibleGPDBVersions(sourceGPHome, targetGPHome string) error {
	sourceVersion, err := GetSourceVersion(sourceGPHome)
	if err != nil {
		return err
	}

	targetVersion, err := GetTargetVersion(targetGPHome)
	if err != nil {
		return err
	}

	return validate(sourceVersion, targetVersion)
}

func validate(sourceVersion DatabaseVersion, targetVersion DatabaseVersion) error {
	var sourceRange, targetRange semver.Range
	var minSourceVersion, minTargetVersion string

	if sourceVersion.Databasetype == Greenplum && targetVersion.Databasetype == Greenplum {
		switch {
		case sourceVersion.Version.Major == 5 && targetVersion.Version.Major == 6:
			sourceRange = semver.MustParseRange(">=" + minGreenplum5xVersion + " <6.0.0")
			targetRange = semver.MustParseRange(">=" + minGreenplum6xVersion + " <7.0.0")
			minSourceVersion = minGreenplum5xVersion
			minTargetVersion = minGreenplum6xVersion
		case sourceVersion.Version.Major == 6 && targetVersion.Version.Major == 6:
			sourceRange = semver.MustParseRange(">=" + minGreenplum6xVersion + " <7.0.0")
			targetRange = semver.MustParseRange(">=" + minGreenplum6xVersion + " <7.0.0")
			minSourceVersion = minGreenplum6xVersion
			minTargetVersion = minGreenplum6xVersion
		case sourceVersion.Version.Major == 6 && targetVersion.Version.Major == 7:
			sourceRange = semver.MustParseRange(">=" + minGreenplum6xVersion + " <7.0.0")
			targetRange = semver.MustParseRange(">=" + minGreenplum7xVersion + " <8.0.0")
			minSourceVersion = minGreenplum6xVersion
			minTargetVersion = minGreenplum7xVersion
		case sourceVersion.Version.Major == 7 && targetVersion.Version.Major == 7:
			sourceRange = semver.MustParseRange(">=" + minGreenplum7xVersion + " <8.0.0")
			targetRange = semver.MustParseRange(">=" + minGreenplum7xVersion + " <8.0.0")
			minSourceVersion = minGreenplum7xVersion
			minTargetVersion = minGreenplum7xVersion
		default:
			return fmt.Errorf("Unsupported source and target versions. "+
				"Found source version %s and target version %s. "+
				"Upgrade is only supported for Greenplum 5 to 6, Greenplum 6 to 7"+
				"Check the documentation for further information.", sourceVersion, targetVersion)
		}

		if !sourceRange(sourceVersion.Version) {
			return fmt.Errorf("Source cluster version %s is not supported. "+
				"The minimum required version is %s. "+
				"We recommend the latest version.", sourceVersion, minSourceVersion)
		}

		if !targetRange(sourceVersion.Version) {
			return fmt.Errorf("Target cluster version %s is not supported. "+
				"The minimum required version is %s. "+
				"We recommend the latest version.", targetVersion, minTargetVersion)
		}
		return nil
	} else if sourceVersion.Databasetype == Cloudberry && targetVersion.Databasetype == Cloudberry {
		if sourceVersion.Version.Major > targetVersion.Version.Major {
			return fmt.Errorf("Unsupported source and target versions. "+
				"TargetVersion %d must larger than sourceVersion %d", targetVersion.Version.Major, sourceVersion.Version.Major)
		}
		return nil
	} else if sourceVersion.Databasetype == Greenplum && targetVersion.Databasetype == Cloudberry {
		return fmt.Errorf("Unsupported source and target versions. "+
			"Upgrade from %s to %s is not supported now", sourceVersion, targetVersion)
	} else if sourceVersion.Databasetype == Cloudberry && targetVersion.Databasetype == Greenplum {

		return fmt.Errorf("Unsupported source and target versions. " +
			"Cannot upgrade from Cloudberry to Greenplum")
	} else {
		return fmt.Errorf("Unsupported source and target versions. "+
			"Upgrade from %s to %s is not supported. "+
			"Check the documentation for further information.", targetVersion, sourceVersion)
	}
}
