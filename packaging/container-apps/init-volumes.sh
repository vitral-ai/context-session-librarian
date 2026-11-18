#!/bin/sh
set -eu
umask 077
test -d /transcripts
test -w /transcripts
test -d /collector-state
test -w /collector-state
