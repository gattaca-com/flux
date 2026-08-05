#!/usr/bin/env python3
"""Validate the workspace version and decide whether HEAD needs a release tag."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SEMVER_PATTERN = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")


class ReleaseError(Exception):
    """A release invariant was not satisfied."""


@dataclass(frozen=True, order=True)
class Version:
    major: int
    minor: int
    patch: int

    @classmethod
    def parse(cls, value: str) -> "Version":
        match = SEMVER_PATTERN.fullmatch(value)
        if match is None:
            raise ReleaseError(
                f"'{value}' is not a stable semantic version; expected MAJOR.MINOR.PATCH"
            )
        return cls(*(int(part) for part in match.groups()))

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


def run(*command: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def load_toml(path: Path) -> dict:
    with path.open("rb") as manifest:
        return tomllib.load(manifest)


def workspace_version() -> Version:
    manifest = load_toml(ROOT / "Cargo.toml")
    try:
        value = manifest["workspace"]["package"]["version"]
    except KeyError as error:
        raise ReleaseError("Cargo.toml has no workspace.package.version") from error
    if not isinstance(value, str):
        raise ReleaseError("workspace.package.version must be a string")
    return Version.parse(value)


def validate_workspace(version: Version) -> None:
    try:
        metadata_result = run(
            "cargo",
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--locked",
        )
    except subprocess.CalledProcessError as error:
        details = error.stderr.strip() or error.stdout.strip()
        raise ReleaseError(f"cargo metadata failed:\n{details}") from error

    metadata = json.loads(metadata_result.stdout)
    members = set(metadata["workspace_members"])
    errors: list[str] = []

    for package in metadata["packages"]:
        if package["id"] not in members:
            continue

        manifest_path = Path(package["manifest_path"])
        package_manifest = load_toml(manifest_path)
        package_table = package_manifest.get("package", {})
        relative_path = manifest_path.relative_to(ROOT)

        for field in ("version", "repository", "license", "publish"):
            if package_table.get(field) != {"workspace": True}:
                errors.append(
                    f"{relative_path}: package.{field} must inherit from the workspace"
                )
        if package["version"] != str(version):
            errors.append(
                f"{relative_path}: effective version {package['version']} does not match {version}"
            )
        # cargo reports `publish = false` as an empty allow-list of registries.
        if package["publish"] != []:
            errors.append(
                f"{relative_path}: effective publish {package['publish']} must be false; "
                "the flux and type-hash names are taken on crates.io"
            )

    if errors:
        raise ReleaseError("workspace package metadata is not synchronized:\n- " + "\n- ".join(errors))


def classify_bump(previous: Version, current: Version) -> str:
    if current <= previous:
        raise ReleaseError(f"version {current} must be greater than released version {previous}")
    if current.major > previous.major and current.minor == 0 and current.patch == 0:
        return "major"
    if (
        current.major == previous.major
        and current.minor > previous.minor
        and current.patch == 0
    ):
        return "minor"
    if (
        current.major == previous.major
        and current.minor == previous.minor
        and current.patch > previous.patch
    ):
        return "patch"
    raise ReleaseError(
        f"version {previous} -> {current} is not a canonical major, minor, or patch bump"
    )


def compatibility_release_type(previous: Version, current: Version, bump: str) -> str:
    """Translate a version bump into Cargo's compatibility boundary."""
    if previous.major == 0:
        if previous.minor == 0 and current.patch != previous.patch:
            return "major"
        if current.major != previous.major or current.minor != previous.minor:
            return "major"
    return bump


def release_tags() -> list[tuple[Version, str]]:
    tags: list[tuple[Version, str]] = []
    for tag in run("git", "tag", "--list", "v*").stdout.splitlines():
        try:
            version = Version.parse(tag.removeprefix("v"))
        except ReleaseError:
            continue
        tags.append((version, tag))
    return sorted(tags)


def assert_tag_is_ancestor(tag: str) -> None:
    result = run("git", "merge-base", "--is-ancestor", tag, "HEAD", check=False)
    if result.returncode != 0:
        raise ReleaseError(f"latest release tag {tag} is not an ancestor of HEAD")


def plan_release() -> dict[str, str]:
    current = workspace_version()
    validate_workspace(current)
    tags = release_tags()
    current_tag = f"v{current}"

    if not tags:
        return {
            "should_release": "true",
            "version": str(current),
            "tag": current_tag,
            "previous_tag": "",
            "bump": "initial",
            "release_type": "major",
        }

    latest_version, latest_tag = tags[-1]
    assert_tag_is_ancestor(latest_tag)

    matching_tag = next((tag for version, tag in tags if version == current), None)
    if matching_tag is not None:
        assert_tag_is_ancestor(matching_tag)
        if latest_version > current:
            raise ReleaseError(
                f"workspace version {current} is older than latest release {latest_version}"
            )
        return {
            "should_release": "false",
            "version": str(current),
            "tag": matching_tag,
            "previous_tag": latest_tag,
            "bump": "none",
            "release_type": "major",
        }

    bump = classify_bump(latest_version, current)
    return {
        "should_release": "true",
        "version": str(current),
        "tag": current_tag,
        "previous_tag": latest_tag,
        "bump": bump,
        "release_type": compatibility_release_type(latest_version, current, bump),
    }


def write_github_output(path: Path, plan: dict[str, str]) -> None:
    with path.open("a", encoding="utf-8") as output:
        for key, value in plan.items():
            output.write(f"{key}={value}\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--github-output",
        type=Path,
        help="append the release plan to this GitHub Actions output file",
    )
    args = parser.parse_args()

    try:
        plan = plan_release()
    except (ReleaseError, OSError, json.JSONDecodeError) as error:
        print(f"release validation failed: {error}", file=sys.stderr)
        return 1

    print(json.dumps(plan, indent=2))
    if args.github_output is not None:
        write_github_output(args.github_output, plan)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
