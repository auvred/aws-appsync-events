{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixpkgs-unstable";
  };
  outputs =
    { self, nixpkgs }:
    let
      eachSystem = nixpkgs.lib.genAttrs [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-darwin"
        "x86_64-linux"
      ];
    in
    {
      devShells = eachSystem (
        system:
        let
          pkgs = import nixpkgs {
            inherit system; 
            overlays = [
              (final: prev: {
                # TODO: remove overlay when nixpkgs provides v6.9.0+
                terraform-providers.aws = prev.terraform-providers.aws.override {
                  rev = "v6.9.0";
                  hash = "sha256-mVIBHoHLj44POScKXxXoXv5w+kq02j047CZvgPGn45Q=";
                  vendorHash = "sha256-DGcgslMH5sZl4FAN2eZf0JRb6SuU8bKUUUeQ5YSm9kg=";
                };
              })
            ];
          };
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              (terraform.withPlugins (p: [ p.aws ]))
            ];
          };
        }
      );
    };
}
