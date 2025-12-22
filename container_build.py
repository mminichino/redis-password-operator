##

import logging
import subprocess
import typer
from pathlib import Path
import tomli

app = typer.Typer()
logger = logging.getLogger()


def build_image(
        repo: str,
        image_name: str,
        version: str,
        platform: str = "linux/amd64"
):
    args = [
        "docker", "buildx", "build",
        "--platform", platform,
        "--sbom=true",
        "--provenance=true",
        "-t", f"{repo}/{image_name}:{version}",
        "--push",
        "."
    ]
    subprocess.run(args, check=True)

@app.command()
def build(
        repo: str = typer.Option(None, "--repo", help="Repository name"),
        image_name: str = typer.Option("redis-password-operator", "--image-name", help="Image name"),
        version: str = typer.Option(None, "--version", help="Version"),
):
    if not version:
        pyproject = Path("pyproject.toml")
        with pyproject.open("rb") as f:
            data = tomli.load(f)
            version = data["project"]["version"]
    build_image(repo, image_name, version)

if __name__ == "__main__":
    app()
