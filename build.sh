#!/usr/bin/env bash
set -e
. jdk21
mvn spotless:apply clean verify