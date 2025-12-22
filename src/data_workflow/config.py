from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)  ## make the dataclass immutable

class Paths:
    root: Path 
    raw: Path 
    processed: Path 
    cache: Path
    external: Path


def make_paths(root: Path) -> Paths:
    data=root / "data"
    return Paths(
        root=data,  
        raw=data / "raw",
        processed=data / "processed",
        cache=data / "cache",
        external=data / "external"
    )