#! /bin/sh

# directory of this script 
DIR="$(cd "$(dirname "$0")" && pwd)"

# remove all raft-*.* files
rm -rf $DIR/e2e/raft-*.*

# remove all raft-* files
rm -rf $DIR/e2e/raft-*

echo "Cleaned up logs in $DIR"
