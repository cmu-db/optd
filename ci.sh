#!/bin/bash
# runs the stuff in CI.yaml locally
# unfortunately this needs to be updated manually. just update it if you get annoyed by GHAs failing

set -e

cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --no-fail-fast --workspace --all-features --locked

# %s is a workaround because printing --- doesn"t work in some shells
# this just makes it more obvious when the CI has passed
printf "%s\n| \033[32m\033[1mCI PASSED\033[0m |\n%s\n" "-------------" "-------------"