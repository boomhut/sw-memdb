# script to set the version label for the current build
# usage: version_label.ps1 <version_label>

param(
    [string]$version_label
)

# set the version label to git tag formatted as: git tag -a "v1.5.0-beta" -m "version v1.5.0-beta"
git tag -a "v$version_label" -m "version v$version_label"
