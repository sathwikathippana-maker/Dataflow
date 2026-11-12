from pathlib import Path
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline_config.yaml"


def load_config(config_path=None):
    path = Path(config_path) if config_path else CONFIG_PATH
    if not path.is_absolute():
        path = PROJECT_ROOT / path

    if not path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {path}"
        )

    with path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError("Invalid configuration file")

    return config


def resolve_path(path):
    """Resolve configured paths relative to the project, not the shell cwd."""
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = PROJECT_ROOT / resolved
    return resolved


if __name__ == "__main__":
    configuration = load_config()
    print("DataFlow Configuration Loaded Successfully")
    print(configuration)
