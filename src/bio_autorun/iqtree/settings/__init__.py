import importlib.util
import logging

logger = logging.getLogger(__name__)


class Settings:
    def __init__(self, settings_path: str):
        spec = importlib.util.spec_from_file_location("settings", settings_path)
        settings = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(settings)
        logger.debug("Settings imported from %s", settings_path)

        self.name: str = settings.NAME
        self.commands: dict[str, str] = settings.COMMANDS
        self.substitution_model: dict[str, str] = settings.MODELS
        self.data_dir: str = settings.DATA_DIR
        self.cwd: str = settings.CWD
        self.output_dir: str = settings.OUTPUT_DIR
        self.seeds: list[int] = settings.SEEDS
        self.time_limit: dict[str, int] = settings.TIME_LIMIT
        self.iterations: dict[str, int] = settings.ITERS
        self.included_data: list[str] = settings.INCLUDED_DATA
        self.excluded_data: list[str] = settings.SKIPPED_DATA
