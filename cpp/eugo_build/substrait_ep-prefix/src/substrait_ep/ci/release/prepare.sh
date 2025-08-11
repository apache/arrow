#!/usr/bin/env bash
# shellcheck shell=bash

set -euo pipefail

# build artifacts
buf build
buf generate
