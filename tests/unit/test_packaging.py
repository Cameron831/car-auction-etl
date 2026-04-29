import importlib
import tomllib
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = PROJECT_ROOT / "pyproject.toml"

MOVED_REQUIREMENTS = {
    "beautifulsoup4==4.13.4",
    "certifi==2026.2.25",
    "charset-normalizer==3.4.6",
    "colorama==0.4.6",
    "idna==3.11",
    "iniconfig==2.3.0",
    "packaging==26.0",
    "playwright==1.58.0",
    "pluggy==1.6.0",
    "psycopg[binary]==3.2.13",
    "Pygments==2.20.0",
    "pytest==9.0.2",
    "pytest-mock==3.15.1",
    "python-dotenv==1.2.1",
    "requests==2.33.1",
    "soupsieve==2.8.3",
    "typing_extensions==4.15.0",
    "urllib3==2.6.3",
}


def load_pyproject():
    return tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))


def test_project_metadata_defines_console_script_entrypoint():
    project = load_pyproject()["project"]

    assert project["name"] == "car-auction-etl"
    assert project["scripts"]["auction-etl"] == "app.cli:main"


def test_console_script_entrypoint_resolves_to_top_level_cli_main():
    script_target = load_pyproject()["project"]["scripts"]["auction-etl"]
    module_name, function_name = script_target.split(":", maxsplit=1)

    resolved = getattr(importlib.import_module(module_name), function_name)

    from app import cli

    assert resolved is cli.main


def test_moved_requirements_are_represented_in_pyproject_dependencies():
    project = load_pyproject()["project"]
    dependencies = set(project["dependencies"])
    for optional_dependencies in project.get("optional-dependencies", {}).values():
        dependencies.update(optional_dependencies)

    assert MOVED_REQUIREMENTS <= dependencies


def test_requirements_txt_points_to_editable_install():
    requirements = (PROJECT_ROOT / "requirements.txt").read_text(encoding="utf-8")

    assert "-e .[test]" in requirements
    assert "beautifulsoup4==" not in requirements
