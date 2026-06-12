{
  inputs.nixpkgs.url = "github:nixos/nixpkgs/release-25.11";

  outputs =
    { self, nixpkgs }:
    {
      packages.x86_64-linux.default =
        let
          pkgs = nixpkgs.legacyPackages.x86_64-linux;
        in
        pkgs.rustPlatform.buildRustPackage {
          pname = "rmd";
          version = "0.1.0";
          src = ./.;
          cargoHash = "sha256-1+cRQ8uSenrjG9+Wg50WaS/FE38zzjUopCsL8IvsVUk=";
        };

      nixosModules.rmd = {
        systemd.user.services.rmd = {
          wantedBy = [ "multi-user.target" ];
          description = "Daemon for mounting a Remarkable tablet as a filesystem.";
          serviceConfig = {
            ExecStart = "${self.packages.x86_64-linux.default}/bin/rmd";
          };

          environment.RUST_LOG = "rmd";
        };
      };
    };
}
