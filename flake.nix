{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
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
          };
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              (terraform.withPlugins (p: [ p.aws ]))
              nodejs_24
            ];
          };
        }
      );
    };
}
