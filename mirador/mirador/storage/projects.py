"""Project storage — JSON-on-disk project and pipeline metadata."""

import json
import time
from pathlib import Path

from pydantic import BaseModel, Field


class ProjectMeta(BaseModel):
    name: str
    slug: str
    created_at: float = Field(default_factory=time.time)


class ProjectStore:
    def __init__(self, root: Path | None = None):
        self.root = root or Path("mirador_data")

    def _projects_dir(self) -> Path:
        d = self.root / "projects"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def list_projects(self) -> list[dict]:
        """List all projects with their metadata."""
        projects = []
        proj_dir = self._projects_dir()
        for p in sorted(proj_dir.iterdir()):
            if p.is_dir():
                meta = self._read_meta(p)
                if meta:
                    projects.append(meta)
        return projects

    def create_project(self, name: str) -> dict:
        """Create a new project directory with metadata."""
        slug = name.lower().replace(" ", "_").replace("-", "_")
        proj_dir = self._projects_dir() / slug
        if proj_dir.exists():
            raise ValueError(f"Project '{slug}' already exists")
        proj_dir.mkdir(parents=True)
        (proj_dir / "data").mkdir()
        (proj_dir / "outputs").mkdir()
        (proj_dir / "pipelines").mkdir()
        (proj_dir / "dashboards").mkdir()
        meta = ProjectMeta(name=name, slug=slug)
        (proj_dir / "meta.json").write_text(meta.model_dump_json(indent=2))
        return meta.model_dump()

    def get_project(self, slug: str) -> dict | None:
        """Get a single project's metadata."""
        proj_dir = self._projects_dir() / slug
        if not proj_dir.exists():
            return None
        return self._read_meta(proj_dir)

    def delete_project(self, slug: str) -> bool:
        """Delete a project and all its contents."""
        import shutil

        proj_dir = self._projects_dir() / slug
        if not proj_dir.exists():
            return False
        shutil.rmtree(proj_dir)
        return True

    def list_pipelines(self, slug: str) -> list[str]:
        """List pipeline names in a project."""
        pipe_dir = self._projects_dir() / slug / "pipelines"
        if not pipe_dir.exists():
            return []
        return [p.stem for p in sorted(pipe_dir.glob("*.json"))]

    def save_pipeline(self, slug: str, pipeline_name: str, pipeline: dict):
        """Save a pipeline definition to a project."""
        pipe_dir = self._projects_dir() / slug / "pipelines"
        pipe_dir.mkdir(parents=True, exist_ok=True)
        path = pipe_dir / f"{pipeline_name}.json"
        path.write_text(json.dumps(pipeline, indent=2))

    def load_pipeline(self, slug: str, pipeline_name: str) -> dict | None:
        """Load a pipeline definition from a project."""
        path = self._projects_dir() / slug / "pipelines" / f"{pipeline_name}.json"
        if not path.exists():
            return None
        return json.loads(path.read_text())

    def delete_pipeline(self, slug: str, pipeline_name: str) -> bool:
        """Delete a pipeline from a project."""
        path = self._projects_dir() / slug / "pipelines" / f"{pipeline_name}.json"
        if not path.exists():
            return False
        path.unlink()
        return True

    # ---- Dashboard CRUD ----

    def list_dashboards(self, slug: str) -> list[str]:
        """List dashboard names in a project."""
        dash_dir = self._projects_dir() / slug / "dashboards"
        if not dash_dir.exists():
            return []
        return [p.stem for p in sorted(dash_dir.glob("*.json"))]

    def save_dashboard(self, slug: str, name: str, data: dict):
        """Save a dashboard definition to a project."""
        dash_dir = self._projects_dir() / slug / "dashboards"
        dash_dir.mkdir(parents=True, exist_ok=True)
        path = dash_dir / f"{name}.json"
        path.write_text(json.dumps(data, indent=2))

    def load_dashboard(self, slug: str, name: str) -> dict | None:
        """Load a dashboard definition from a project."""
        path = self._projects_dir() / slug / "dashboards" / f"{name}.json"
        if not path.exists():
            return None
        return json.loads(path.read_text())

    def delete_dashboard(self, slug: str, name: str) -> bool:
        """Delete a dashboard from a project."""
        path = self._projects_dir() / slug / "dashboards" / f"{name}.json"
        if not path.exists():
            return False
        path.unlink()
        return True

    def _read_meta(self, proj_dir: Path) -> dict | None:
        meta_file = proj_dir / "meta.json"
        if meta_file.exists():
            return json.loads(meta_file.read_text())
        return None
