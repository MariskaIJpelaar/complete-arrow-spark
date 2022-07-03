#!/usr/bin/env bash

# Installs mvn locally in the build directory
if [[ -z $BASH_SOURCE ]]; then
  echo "Expected BASH_SOURCE to be non-empty"
  exit 1
fi

# Get path to script
scriptpath="$(dirname "$(realpath "$BASH_SOURCE")")"

# Creates the build-directory if it does not exist yet
mkdir -p "$scriptpath/build"

# Downloads maven 3.8.6
maventar=$scriptpath/build/apache-maven-3.8.6-bin.tar.gz
if [[ ! -f "$maventar" ]]; then
  wget --quiet -P "$scriptpath/build" https://dlcdn.apache.org/maven/maven-3/3.8.6/binaries/apache-maven-3.8.6-bin.tar.gz
  tar --directory "$scriptpath"/build -xf "$maventar"
fi

mavenbin=$scriptpath/build/apache-maven-3.8.6/bin/mvn
if [[ ! -f "$mavenbin" ]]; then
  echo "Something went wrong with installing mvn"
  exit 1
fi

mavensym=$scriptpath/build/mvn
if [[ ! -f "$mavensym" ]]; then
  ln -s "$mavenbin" "$mavensym"
fi

$mavensym -v
exitcode=$?
test $exitcode -eq 0 || echo "Something went wrong with creating the symbolic link"
exit $exitcode


