# HPACK provenance

Copied from hyperium/h2 0.4.17, commit 1adb037a341ec05b0407e7c627a06cc507cc78f7.
Source: https://github.com/hyperium/h2/tree/1adb037a341ec05b0407e7c627a06cc507cc78f7/src/hpack
The MIT license is preserved in LICENSE.

Changes: module imports point to flux_network::http2::hpack; the HTTP/2 frame-error
conversion is removed; ext::Protocol is included locally. Inline upstream unit
tests are retained. External fixture and QuickCheck runners are not copied.
The wrapper module suppresses Flux-specific lints to keep upstream code intact.
