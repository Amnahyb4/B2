from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)  ## make the dataclass immutable

class Paths:
    root: Path 
    data: Path
    raw: Path 
    processed: Path 
    cache: Path
    external: Path
    reports: Path 


def make_paths(root: Path) -> Paths:
    data=root / "data"
    return Paths(
        root=root,  
        data=data,
        raw=data / "raw",
        processed=data / "processed",
        cache=data / "cache",
        external=data / "external", 
        reports=root / "reports",
    )