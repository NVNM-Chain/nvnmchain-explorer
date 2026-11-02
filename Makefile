.PHONY: build check test install uninstall clean

# Lightweight deploy targets. `make install` builds the release binary,
# installs it under /opt/nvnmchain-explorer and registers a hardened systemd
# service (see deploy/).

build:
	cargo build --release --locked

check:
	cargo fmt --check
	cargo clippy --all-targets -- -D warnings

test:
	cargo test --test decoder
	cargo test --test baseline
	cargo test --test live_rpc

install: build
	./deploy/install.sh

uninstall:
	./deploy/install.sh --uninstall

clean:
	cargo clean
