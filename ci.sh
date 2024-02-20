#!/bin/bash
# runs the stuff in CI.yaml locally
# unfortunately this needs to be updated manually. just update it if you get annoyed by GHAs failing

set -ex

cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --no-fail-fast --workspace --all-features --locked

if [ "$?" -eq 0 ]; then
  echo "PASSED"
else
  echo "FAILED"
fi