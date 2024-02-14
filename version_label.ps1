# script to set the version label for the current build
# usage: version_label.ps1 <version_label>

param(
    [string]$version_label
)

# set the version label to git tag
git tag $version_label
