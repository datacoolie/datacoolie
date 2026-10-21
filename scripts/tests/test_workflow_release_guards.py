"""Verify credential boundaries and release guards in checked-in workflows."""

from pathlib import Path
import subprocess
import sys

import yaml


ROOT = Path(__file__).resolve().parents[2]


def workflow(name: str) -> dict:
    # BaseLoader keeps the Actions `on` key literal instead of YAML 1.1 boolean.
    return yaml.load((ROOT / ".github/workflows" / name).read_text(), Loader=yaml.BaseLoader)


def test_ci_installs_avro_fixture_dependency_before_ai_gate() -> None:
    requirements = (ROOT / "scripts/requirements-release.txt").read_text().splitlines()
    assert "fastavro==1.12.2" in requirements
    steps = workflow("ci.yml")["jobs"]["validate"]["steps"]
    install = next(i for i, step in enumerate(steps) if "requirements-release.txt" in step.get("run", ""))
    ai_gate = next(i for i, step in enumerate(steps) if "ai/skills/tests/run_all.py" in step.get("run", ""))
    assert install < ai_gate
    publishing = workflow("publish-pypi.yml")["jobs"]["publish"]["steps"]
    assert any("requirements-release.txt" in step.get("run", "") for step in publishing)


def test_docs_build_and_deployment_use_separate_credentials() -> None:
    docs = workflow("docs.yml")
    assert docs["permissions"] == {"contents": "read"}
    build = docs["jobs"]["build"]
    assert "permissions" not in build
    checkout = build["steps"][0]
    assert checkout["with"]["persist-credentials"] == "false"
    assert "token" not in checkout["with"]
    assert "secrets." not in yaml.dump(build)
    upload = next(s for s in build["steps"] if s.get("uses", "").startswith("actions/upload-artifact@"))
    assert upload["with"]["if-no-files-found"] == "error"

    deploy = docs["jobs"]["deploy"]
    assert deploy["needs"] == "build"
    assert deploy["if"] == "github.event_name == 'push' && github.ref == 'refs/heads/main'"
    assert deploy["permissions"] == {"contents": "write"}
    download = next(s for s in deploy["steps"] if s.get("uses", "").startswith("actions/download-artifact@"))
    assert upload["with"]["name"] == download["with"]["name"]
    assert upload["with"]["path"] == download["with"]["path"] == "site/"
    runs = [s["run"] for s in deploy["steps"] if "run" in s]
    assert "python -m pip install ghp-import==2.1.0" in runs
    assert runs[-1] == "python -m ghp_import --push --force --no-jekyll --branch gh-pages site"
    assert all("properdocs" not in run for run in runs)


def test_both_release_workflows_check_refs_before_publication() -> None:
    for name in ("publish-pypi.yml", "release.yml"):
        config = workflow(name)
        assert config["on"]["push"]["tags"] == ["v*"]
        job = next(iter(config["jobs"].values()))
        steps = job["steps"]
        assert steps[0]["uses"].startswith("actions/checkout@")
        assert steps[0]["with"]["fetch-depth"] == "0"
        assert steps[0]["with"]["persist-credentials"] == "false"
        guard = steps[1]
        assert guard["env"]["RELEASE_TAG"] == "${{ github.ref_name }}"
        assert guard["run"] == 'python3 scripts/verify_release_ref.py --tag "$RELEASE_TAG"'
        for step in steps:
            assert "${{ github.ref_name }}" not in step.get("run", "")
        if name == "publish-pypi.yml":
            gate = next(s for s in steps if s.get("name") == "Run release gate for tag")
            assert gate["env"]["RELEASE_TAG"] == "${{ github.ref_name }}"
            assert gate["run"].endswith('--tag "$RELEASE_TAG"')


def test_deployment_command_publishes_verified_tree_to_local_remote(tmp_path: Path) -> None:
    remote = tmp_path / "remote.git"
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True)
    for args in (
        ["init", "--initial-branch", "main"],
        ["config", "user.name", "docs-deploy-test"],
        ["config", "user.email", "docs-deploy@example.invalid"],
        ["remote", "add", "origin", str(remote)],
        ["commit", "--allow-empty", "-m", "base"],
    ):
        subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)
    site = repo / "site"
    site.mkdir()
    (site / "index.html").write_text("verified artifact\n", encoding="utf-8")
    (site / "assets").mkdir()
    (site / "assets" / "app.css").write_text("body {}\n", encoding="utf-8")
    subprocess.run(
        [sys.executable, "-m", "ghp_import", "--push", "--force", "--no-jekyll",
         "--branch", "gh-pages", "site"],
        cwd=repo, check=True, capture_output=True,
    )
    published = subprocess.check_output(
        ["git", "--git-dir", str(remote), "ls-tree", "--name-only", "-r", "gh-pages"],
        text=True,
    ).splitlines()
    assert set(published) == {".nojekyll", "index.html", "assets/app.css"}
    index = subprocess.check_output(
        ["git", "--git-dir", str(remote), "show", "gh-pages:index.html"], text=True
    )
    assert index == (site / "index.html").read_text(encoding="utf-8")
