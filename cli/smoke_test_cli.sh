#!/usr/bin/env bash
# CLI smoke test - verifies catalog integration is active

set -e  # Exit on error

GREEN='\033[0;32m'
RED='\033[0;31m'
RESET='\033[0m'

echo "=== CLI Smoke Test ==="

# Build
echo "Building..."
cargo build --package optd-cli --quiet
if [ ! -f ./target/debug/optd-cli ]; then
    echo -e "${RED}✗ Build failed${RESET}"
    exit 1
fi

CLI=./target/debug/optd-cli

# Test 1: Basic functionality
echo "Test 1: Basic query execution"
output=$($CLI -c "SELECT 1 as test;" 2>&1)
if [ $? -eq 0 ] && echo "$output" | grep -q "OptD catalog"; then
    echo -e "${GREEN}✓ PASS${RESET} - CLI runs, catalog integration active"
else
    echo -e "${RED}✗ FAIL${RESET}"
    exit 1
fi

# Test 2: Session persistence (multiple commands)
echo "Test 2: Session state persistence"
output=$($CLI -c "CREATE TABLE t (x INT);" -c "INSERT INTO t VALUES (1);" -c "SELECT * FROM t;" 2>&1)
if [ $? -eq 0 ] && echo "$output" | grep -q "1 row"; then
    echo -e "${GREEN}✓ PASS${RESET} - Multiple commands work, session persists"
else
    echo -e "${RED}✗ FAIL${RESET}"
    exit 1
fi

# Test 3: Metadata path configuration
echo "Test 3: Metadata path environment variable"
TMPDIR_PATH=$(mktemp -d)
export OPTD_CATALOG_METADATA_PATH="$TMPDIR_PATH/test.ducklake"
output=$($CLI -c "SELECT 1;" 2>&1)
unset OPTD_CATALOG_METADATA_PATH
rm -rf "$TMPDIR_PATH"
if echo "$output" | grep -q "Using OptD catalog with metadata path"; then
    echo -e "${GREEN}✓ PASS${RESET} - Metadata path recognized"
else
    echo -e "${RED}✗ FAIL${RESET}"
    exit 1
fi

echo ""
echo -e "${GREEN}✓ All smoke tests passed!${RESET}"
