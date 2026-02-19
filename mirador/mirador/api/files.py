"""File browser API — lists directories and CSV files for the frontend."""

from pathlib import Path

from fastapi import APIRouter, Query

router = APIRouter(prefix="/api/files", tags=["files"])


@router.get("/browse")
def browse_directory(path: str = Query("~", description="Directory to list")):
    """Return directories and data files in the given directory."""
    target = Path(path).expanduser().resolve()

    if not target.exists():
        return {"path": str(target), "error": "Directory not found", "entries": []}
    if not target.is_dir():
        return {"path": str(target), "error": "Not a directory", "entries": []}

    entries = []
    try:
        for item in sorted(target.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
            if item.name.startswith("."):
                continue
            if item.is_dir():
                entries.append({"name": item.name, "type": "dir", "path": str(item)})
            elif item.suffix.lower() in (".csv", ".tsv", ".parquet", ".json"):
                size = item.stat().st_size
                entries.append({
                    "name": item.name,
                    "type": "file",
                    "path": str(item),
                    "size": size,
                })
    except PermissionError:
        return {"path": str(target), "error": "Permission denied", "entries": []}

    parent = str(target.parent) if target.parent != target else None
    return {"path": str(target), "parent": parent, "entries": entries}
