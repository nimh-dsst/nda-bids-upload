import subprocess
from pathlib import Path
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

class LoadSubmodules(BuildHookInterface):
    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        root = Path(self.root)
        if (root / '.gitmodules').exists():
            subprocess.run(
                ['git',
                'submodule',
                'update',
                '--init', 
                'manifest-data'],
                cwd=root,
                capture_output=True
            ),
            subprocess.run(
                ['git',
                'submodule',
                'update',
                '--init', 
                'tests/bids-examples'],
                cwd=root,
                capture_output=True
            ),
            