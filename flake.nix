{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            buf
            protobuf
            go
          ];
          shellHook = '' 
            export PS1="(wireless-networks) $PS1"
            export PATH="$PATH:$(go env GOPATH)/bin"
            go install github.com/bufbuild/buf/cmd/buf@latest
            go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
            go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
          '';
        };
      }
    );
}
