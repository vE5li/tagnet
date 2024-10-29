self: {
  pkgs,
  lib,
  config,
  ...
}: {
  options.services.tagnet = with lib; {
    # enable = lib.mkEnableOption "tagnet service";

    user = mkOption {
      type = types.str;
      description = "User account under which tagnet runs.";
    };

    group = mkOption {
      type = types.str;
      description = "Group under which tagnet runs.";
    };

    package = mkOption {
      type = types.package;
      default = self.packages.${pkgs.system}.default;
      defaultText = literalExpression "tagnet.packages.\${system}.default";
      description = "The tagnet package to use.";
    };

    configuration-file = mkOption {
      type = types.path;
      description = "Path to the configuration file";
    };

    state-directory = mkOption {
      type = types.str;
      default = "tagnet";
      description = ''
        Name of the systemd StateDirectory, created under /var/lib and
        owned by the service user. Used as the default data directory.
      '';
    };

    data-directory = mkOption {
      type = types.path;
      default = "/var/lib/${config.services.tagnet.state-directory}";
      defaultText = literalExpression ''"/var/lib/''${state-directory}"'';
      description = "Path to the data directory";
    };

    private-key-file = mkOption {
      type = types.path;
      description = "Path to the private key file";
    };
  };

  config = with config.services.tagnet; {
    systemd.services.tagnet = {
      enable = true;

      wantedBy = ["multi-user.target"];
      after = ["network.target"];

      serviceConfig = {
        ExecStart = "${lib.getExe package} run ${configuration-file}";
        Restart = "on-failure";
        RestartSec = 5;
        User = user;
        Group = group;
        StateDirectory = state-directory;
      };

      environment = {
        RUST_LOG = "debug";
        TAGNET_DATA_DIR = "${data-directory}";
        TAGNET_PRIVATE_KEY_FILE = "${private-key-file}";
      };
    };

    # TODO: Put behind proper option.
    networking.firewall = {
      allowedTCPPorts = [3468];
    };
  };
}
