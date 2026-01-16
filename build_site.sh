#!/bin/bash

# builds a repository of scrapers
# outputs to _site with the following structure:
# index.yml
# <scraper_id>.zip
# Each zip file contains the scraper.yml file and any other files in the same directory

outdir="$1"
if [ -z "$outdir" ]; then
    outdir="_site"
fi

shopt -s nullglob

rm -rf "$outdir"
mkdir -p "$outdir"

# Plugins that should only be built from their dist directory (TypeScript projects)
# Add plugin directory names here to use dist/ as the source
DIST_ONLY_PLUGINS=(
    "linkTagsToPage"
)

# Check if a plugin should use dist directory
is_dist_only_plugin() {
    local plugin_dir=$1
    for p in "${DIST_ONLY_PLUGINS[@]}"; do
        if [ "$p" == "$plugin_dir" ]; then
            return 0
        fi
    done
    return 1
}

buildPlugin() 
{
    f=$1

    if grep -q "^#pkgignore" "$f"; then
        return
    fi
    
    # get the scraper id from the directory
    dir=$(dirname "$f")
    plugin_id=$(basename "$f" .yml)
    plugin_dir_name=$(basename "$dir")
    
    # For dist-only plugins, skip if this yml is not in a dist directory
    if is_dist_only_plugin "$plugin_dir_name"; then
        if [[ "$dir" != */dist ]]; then
            return
        fi
    fi

    echo "Processing $plugin_id"

    # Determine the version source directory (parent for dist plugins, same dir otherwise)
    if [[ "$dir" == */dist ]]; then
        version_dir=$(dirname "$dir")
    else
        version_dir="$dir"
    fi

    # create a directory for the version
    version=$(git log -n 1 --pretty=format:%h -- "$version_dir"/*)
    updated=$(TZ=UTC0 git log -n 1 --date="format-local:%F %T" --pretty=format:%ad -- "$version_dir"/*)
    
    # create the zip file
    # copy other files
    zipfile=$(realpath "$outdir/$plugin_id.zip")
    
    pushd "$dir" > /dev/null
    find . -type f -exec touch -d "$updated" {} +
    grep -rl . | sort | zip -0 -r -oX "$zipfile" -@ > /dev/null
    popd > /dev/null

    name=$(grep "^name:" "$f" | head -n 1 | cut -d' ' -f2- | sed -e 's/\r//' -e 's/^"\(.*\)"$/\1/')
    description=$(grep "^description:" "$f" | head -n 1 | cut -d' ' -f2- | sed -e 's/\r//' -e 's/^"\(.*\)"$/\1/')
    ymlVersion=$(grep "^version:" "$f" | head -n 1 | cut -d' ' -f2- | sed -e 's/\r//' -e 's/^"\(.*\)"$/\1/')
    version="$ymlVersion-$version"
    dep=$(grep "^# requires:" "$f" | cut -c 12- | sed -e 's/\r//')

    # write to spec index
    echo "- id: $plugin_id
  name: $name
  metadata:
    description: $description
  version: $version
  date: $updated
  path: $plugin_id.zip
  sha256: $(sha256sum "$zipfile" | cut -d' ' -f1)" >> "$outdir"/index.yml

    # handle dependencies
    if [ ! -z "$dep" ]; then
        echo "  requires:" >> "$outdir"/index.yml
        for d in ${dep//,/ }; do
            echo "    - $d" >> "$outdir"/index.yml
        done
    fi

    echo "" >> "$outdir"/index.yml
}

# Find yml files in plugins (including dist subdirectories for TypeScript projects)
find ./plugins -mindepth 2 -name "*.yml" | sort | while read file; do
    buildPlugin "$file"
done
find ./themes -mindepth 2 -name "*.yml" | sort | while read file; do
    buildPlugin "$file"
done

buildScraper() 
{
    f=$1
    dir=$(dirname "$f")

    # get the scraper id from the filename
    scraper_id=$(basename "$f" .yml)
    versionFile=$f
    if [ "$scraper_id" == "package" ]; then
        scraper_id=$(basename "$dir")
    fi

    if [ "$dir" != "./scrapers" ]; then
        versionFile="$dir"
    fi

    echo "Processing $scraper_id"

    # create a directory for the version
    version=$(git log -n 1 --pretty=format:%h -- "$versionFile")
    updated=$(TZ=UTC0 git log -n 1 --date="format-local:%F %T" --pretty=format:%ad -- "$versionFile")
    
    # create the zip file
    # copy other files
    zipfile=$(realpath "$outdir/$scraper_id.zip")

    name=$(grep "^name:" "$f" | cut -d' ' -f2- | sed -e 's/\r//' -e 's/^"\(.*\)"$/\1/')
    ignore=$(grep "^# ignore:" "$f" | cut -c 10- | sed -e 's/\r//')
    dep=$(grep "^# requires:" "$f" | cut -c 12- | sed -e 's/\r//')

    # always ignore package file
    ignore="-x $ignore package"

    pushd "$dir" > /dev/null
    if [ "$dir" != "./scrapers" ]; then
        zip -r "$zipfile" . ${ignore} > /dev/null
    else
        zip "$zipfile" "$scraper_id.yml" > /dev/null
    fi
    popd > /dev/null

    # write to spec index
    echo "- id: $scraper_id
  name: $name
  version: $version
  date: $updated
  path: $scraper_id.zip
  sha256: $(sha256sum "$zipfile" | cut -d' ' -f1)" >> "$outdir"/index.yml

    # handle dependencies
    if [ ! -z "$dep" ]; then
        echo "  requires:" >> "$outdir"/index.yml
        for d in ${dep//,/ }; do
            echo "    - $d" >> "$outdir"/index.yml
        done
    fi

    echo "" >> "$outdir"/index.yml
}

find ./scrapers/ -mindepth 2 -name "*.yml" -print0 | while read -d $'\0' f; do
    buildScraper "$f"
done