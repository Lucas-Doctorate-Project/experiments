{
  pkgs ? import (fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/11cb3517b3af6af300dd6c055aeda73c9bf52c48.tar.gz";
    sha256 = "1915r28xc4znrh2vf4rrjnxldw2imysz819gzhk9qlrkqanmfsxd";
  }) { },
  kapack ? import (fetchTarball {
    url = "https://github.com/oar-team/nur-kapack/archive/774b7f56c3efc93adb73ee82a62faf5dc2fce112.tar.gz";
    sha256 = "1dm41i91apa6kvc8wcmliwy81r71k2mg002s4przihgml5zcnyfs";
  }) { },
}:

let

  my-simgrid = kapack.simgrid-400light.overrideAttrs {
    src = pkgs.fetchFromGitHub {
      owner = "Lucas-Doctorate-Project";
      repo = "simgrid";
      rev = "f0871456f14792caa132fbb0f8cc59331ffe312c";
      hash = "sha256-73aprhAXdafYuZ6pkrfcPz3We8wB5WbLAFhyY0Z+zew=";
    };
  };
  my-batsim =
    (kapack.batsim.override {
      simgrid = my-simgrid;
    }).overrideAttrs
      {
        src = pkgs.fetchFromGitHub {
          owner = "Lucas-Doctorate-Project";
          repo = "batsim";
          rev = "67d11f192a8f7d7276f28452b75323f3b0af86f7";
          hash = "sha256-ujaG38b9hc2p+3uW/TU7HQUi7gdaCnxhrx8qhZp2luY=";
        };
      };
  my-batsched = kapack.batsched.overrideAttrs {
    src = pkgs.fetchFromGitHub {
      owner = "Lucas-Doctorate-Project";
      repo = "batsched";
      rev = "84910fc22c4d6b703a610493f10d27ad469237a9";
      hash = "sha256-qmIssnWiB3wTQ0zYGYQ+JzEEzvz6PakcicmWOvU8DcE=";
    };
  };

in
pkgs.mkShell {
  packages = [
    my-simgrid
    my-batsim
    my-batsched
    (pkgs.python3.withPackages (python-pkgs: [
      python-pkgs.pandas
      python-pkgs.requests
    ]))
  ];
  DYLD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [ kapack.loguru ];
}
