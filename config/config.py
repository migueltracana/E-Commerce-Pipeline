import os
import yaml

class ConfigError(Exception):
    pass

def load_config(env="dev"):
    """
    Load YAML config file based on environment.
    Validate required fields.
    AWS credentials are handled automatically by boto3 (profile, env vars, IAM).
    """
    config_file = os.path.join(os.path.dirname(__file__), f"{env}.yaml")

    if not os.path.exists(config_file):
        raise ConfigError(f"Config file not found: {config_file}")

    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    # Replace environment variables
    for section in config:
        for key, value in config[section].items():
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                env_var = value[2:-1]
                env_value = os.environ.get(env_var)
                if not env_value:
                    raise ConfigError(f"Missing environment variable: {env_var}")
                config[section][key] = env_value

    # Validate database config
    required_db_keys = ["user", "password", "host", "port", "name"]
    for key in required_db_keys:
        if key not in config.get("database", {}):
            raise ConfigError(f"Missing database config key: {key}")

    # Validate AWS config (simplified)
    aws_config = config.get("aws", {})
    for required_key in ["region", "bucket"]:
        if required_key not in aws_config:
            raise ConfigError(f"Missing AWS config key: {required_key}")

    # Remove optional keys if still present (legacy)
    aws_config.pop("access_key", None)
    aws_config.pop("secret_key", None)

    return config
