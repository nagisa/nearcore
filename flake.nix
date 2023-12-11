# This is a simple deterministic rust development environment
# This exposes Cargo, rustfmt, rust-analyzer and clippy
# This does not allow you to build binaries using nix
{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };
  outputs = { self, nixpkgs, flake-utils, rust-overlay, ... }:

    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };

        # Pick what rust compiler to use
      in {
        devShell = pkgs.mkShell {

          # Everything in this list is added to your path
          buildInputs =
            with pkgs;
            [
              protobuf
              curl
              gmp
              openssl
              cmake
              clang
              iconv

              zlib
              zstd
              bzip2
              lz4
              snappy
              pkg-config

            ] ++

            pkgs.lib.optionals pkgs.stdenv.isDarwin
              [
              # Mac crypto libs
              darwin.apple_sdk.frameworks.Security
              darwin.apple_sdk.frameworks.SystemConfiguration
              darwin.apple_sdk.frameworks.Foundation
              darwin.apple_sdk.frameworks.CoreFoundation
              ];
        };
      });
}
