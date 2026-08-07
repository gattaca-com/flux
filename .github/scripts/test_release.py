#!/usr/bin/env python3

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).with_name("release.py")
SPEC = importlib.util.spec_from_file_location("release", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
release = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = release
SPEC.loader.exec_module(release)


class VersionTests(unittest.TestCase):
    def test_accepts_stable_semver(self) -> None:
        self.assertEqual(str(release.Version.parse("12.34.56")), "12.34.56")

    def test_rejects_prerelease_and_leading_zero(self) -> None:
        for value in ("1.2", "1.2.3-alpha", "01.2.3"):
            with self.subTest(value=value), self.assertRaises(release.ReleaseError):
                release.Version.parse(value)

    def test_classifies_canonical_bumps(self) -> None:
        cases = {
            ("1.2.3", "1.2.4"): "patch",
            ("1.2.3", "1.3.0"): "minor",
            ("1.2.3", "2.0.0"): "major",
            ("0.1.9", "0.2.0"): "minor",
        }
        for (old, new), expected in cases.items():
            with self.subTest(old=old, new=new):
                self.assertEqual(
                    release.classify_bump(
                        release.Version.parse(old), release.Version.parse(new)
                    ),
                    expected,
                )

    def test_rejects_noncanonical_bumps(self) -> None:
        for old, new in (("1.2.3", "1.2.3"), ("1.2.3", "1.3.1"), ("1.2.3", "2.1.0")):
            with self.subTest(old=old, new=new), self.assertRaises(release.ReleaseError):
                release.classify_bump(release.Version.parse(old), release.Version.parse(new))

    def test_pre_one_compatibility_boundaries_are_breaking(self) -> None:
        cases = {
            ("0.1.1", "0.1.2"): "patch",
            ("0.1.1", "0.2.0"): "major",
            ("0.0.1", "0.0.2"): "major",
            ("1.1.0", "1.2.0"): "minor",
        }
        for (old, new), expected in cases.items():
            with self.subTest(old=old, new=new):
                previous = release.Version.parse(old)
                current = release.Version.parse(new)
                bump = release.classify_bump(previous, current)
                self.assertEqual(
                    release.compatibility_release_type(previous, current, bump), expected
                )


class WorkspaceValidationTests(unittest.TestCase):
    def test_accepts_inherited_workspace_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "crates" / "example" / "Cargo.toml"
            manifest.parent.mkdir(parents=True)
            manifest.write_text(
                """\
[package]
name = "example"
version.workspace = true
repository.workspace = true
license.workspace = true
publish.workspace = true
""",
                encoding="utf-8",
            )
            metadata = {
                "workspace_members": ["example-id"],
                "packages": [
                    {
                        "id": "example-id",
                        "manifest_path": str(manifest),
                        "version": "0.1.0",
                        "publish": [],
                    }
                ],
            }
            result = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=json.dumps(metadata), stderr=""
            )

            with (
                mock.patch.object(release, "ROOT", root),
                mock.patch.object(release, "run", return_value=result) as run,
            ):
                release.validate_workspace(release.Version.parse("0.1.0"))

            run.assert_called_once_with(
                "cargo",
                "metadata",
                "--format-version",
                "1",
                "--no-deps",
                "--locked",
            )

    def test_rejects_package_metadata_that_is_not_inherited(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "crates" / "example" / "Cargo.toml"
            manifest.parent.mkdir(parents=True)
            manifest.write_text(
                """\
[package]
name = "example"
version.workspace = true
repository.workspace = true
license.workspace = true
publish = false
""",
                encoding="utf-8",
            )
            metadata = {
                "workspace_members": ["example-id"],
                "packages": [
                    {
                        "id": "example-id",
                        "manifest_path": str(manifest),
                        "version": "0.1.0",
                        "publish": [],
                    }
                ],
            }
            result = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=json.dumps(metadata), stderr=""
            )

            with (
                mock.patch.object(release, "ROOT", root),
                mock.patch.object(release, "run", return_value=result),
                self.assertRaisesRegex(release.ReleaseError, "package.publish"),
            ):
                release.validate_workspace(release.Version.parse("0.1.0"))


class GitReleaseTests(unittest.TestCase):
    def test_release_tags_returns_only_stable_version_tags(self) -> None:
        result = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="v2.0.0\nv0.2.0\nv1.0.0-rc.1\nnot-a-release\n",
            stderr="",
        )
        with mock.patch.object(release, "run", return_value=result):
            self.assertEqual(
                release.release_tags(),
                [
                    (release.Version.parse("0.2.0"), "v0.2.0"),
                    (release.Version.parse("2.0.0"), "v2.0.0"),
                ],
            )

    def test_ancestor_check_rejects_unrelated_tag(self) -> None:
        result = subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="")
        with (
            mock.patch.object(release, "run", return_value=result) as run,
            self.assertRaisesRegex(release.ReleaseError, "not an ancestor"),
        ):
            release.assert_tag_is_ancestor("v1.2.3")
        run.assert_called_once_with(
            "git", "merge-base", "--is-ancestor", "v1.2.3", "HEAD", check=False
        )

    def test_plan_bootstraps_when_no_tags_exist(self) -> None:
        current = release.Version.parse("0.1.0")
        with (
            mock.patch.object(release, "workspace_version", return_value=current),
            mock.patch.object(release, "validate_workspace") as validate,
            mock.patch.object(release, "release_tags", return_value=[]),
        ):
            plan = release.plan_release()

        validate.assert_called_once_with(current)
        self.assertEqual(plan["should_release"], "true")
        self.assertEqual(plan["tag"], "v0.1.0")
        self.assertEqual(plan["bump"], "initial")

    def test_plan_skips_an_already_released_version(self) -> None:
        current = release.Version.parse("0.1.0")
        with (
            mock.patch.object(release, "workspace_version", return_value=current),
            mock.patch.object(release, "validate_workspace"),
            mock.patch.object(release, "release_tags", return_value=[(current, "v0.1.0")]),
            mock.patch.object(release, "assert_tag_is_ancestor") as ancestor,
        ):
            plan = release.plan_release()

        ancestor.assert_has_calls([mock.call("v0.1.0"), mock.call("v0.1.0")])
        self.assertEqual(plan["should_release"], "false")
        self.assertEqual(plan["tag"], "v0.1.0")

    def test_plan_classifies_the_next_release(self) -> None:
        previous = release.Version.parse("0.1.0")
        current = release.Version.parse("0.1.1")
        with (
            mock.patch.object(release, "workspace_version", return_value=current),
            mock.patch.object(release, "validate_workspace"),
            mock.patch.object(
                release, "release_tags", return_value=[(previous, "v0.1.0")]
            ),
            mock.patch.object(release, "assert_tag_is_ancestor"),
        ):
            plan = release.plan_release()

        self.assertEqual(plan["should_release"], "true")
        self.assertEqual(plan["previous_tag"], "v0.1.0")
        self.assertEqual(plan["bump"], "patch")
        self.assertEqual(plan["release_type"], "patch")

    def test_plan_rejects_a_version_older_than_latest_tag(self) -> None:
        current = release.Version.parse("0.1.0")
        latest = release.Version.parse("0.2.0")
        with (
            mock.patch.object(release, "workspace_version", return_value=current),
            mock.patch.object(release, "validate_workspace"),
            mock.patch.object(
                release,
                "release_tags",
                return_value=[(current, "v0.1.0"), (latest, "v0.2.0")],
            ),
            mock.patch.object(release, "assert_tag_is_ancestor"),
            self.assertRaisesRegex(release.ReleaseError, "older than latest release"),
        ):
            release.plan_release()


class LocalTagTests(unittest.TestCase):
    @staticmethod
    def result(stdout: str = "", returncode: int = 0) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=[], returncode=returncode, stdout=stdout, stderr=""
        )

    def test_prepare_local_tag_requires_clean_main_at_origin_main(self) -> None:
        plan = {"should_release": "true", "version": "0.2.0", "tag": "v0.2.0"}
        with (
            mock.patch.object(
                release,
                "run",
                side_effect=[
                    self.result(),
                    self.result("main\n"),
                    self.result(),
                    self.result("abc123\n"),
                    self.result("abc123\n"),
                ],
            ) as run,
            mock.patch.object(release, "plan_release", return_value=plan),
        ):
            self.assertEqual(release.prepare_local_tag(), plan)

        self.assertEqual(
            run.call_args_list,
            [
                mock.call("git", "status", "--porcelain", check=True),
                mock.call(
                    "git", "symbolic-ref", "--quiet", "--short", "HEAD", check=False
                ),
                mock.call("git", "fetch", "--tags", "origin", check=True),
                mock.call("git", "rev-parse", "HEAD", check=True),
                mock.call(
                    "git", "rev-parse", "refs/remotes/origin/main", check=True
                ),
            ],
        )

    def test_prepare_local_tag_rejects_a_dirty_worktree(self) -> None:
        with (
            mock.patch.object(
                release, "run", return_value=self.result(" M Cargo.toml\n")
            ),
            self.assertRaisesRegex(release.ReleaseError, "worktree must be clean"),
        ):
            release.prepare_local_tag()

    def test_prepare_local_tag_rejects_detached_head(self) -> None:
        with (
            mock.patch.object(
                release,
                "run",
                side_effect=[self.result(), self.result(returncode=1)],
            ),
            self.assertRaisesRegex(release.ReleaseError, "detached HEAD"),
        ):
            release.prepare_local_tag()

    def test_prepare_local_tag_rejects_an_outdated_main(self) -> None:
        with (
            mock.patch.object(
                release,
                "run",
                side_effect=[
                    self.result(),
                    self.result("main\n"),
                    self.result(),
                    self.result("local\n"),
                    self.result("remote\n"),
                ],
            ),
            self.assertRaisesRegex(release.ReleaseError, "exactly match origin/main"),
        ):
            release.prepare_local_tag()

    def test_create_and_push_tag_creates_an_annotated_tag(self) -> None:
        with mock.patch.object(release, "run", return_value=self.result()) as run:
            release.create_and_push_tag(
                {"should_release": "true", "version": "0.2.0", "tag": "v0.2.0"}
            )

        self.assertEqual(
            run.call_args_list,
            [
                mock.call(
                    "git",
                    "tag",
                    "--annotate",
                    "v0.2.0",
                    "--message",
                    "Release v0.2.0",
                    check=True,
                ),
                mock.call(
                    "git", "push", "origin", "refs/tags/v0.2.0", check=True
                ),
            ],
        )

    def test_create_and_push_tag_removes_local_tag_when_push_fails(self) -> None:
        failure = subprocess.CalledProcessError(
            returncode=128,
            cmd=["git", "push"],
            stderr="push rejected",
        )
        with (
            mock.patch.object(
                release,
                "run",
                side_effect=[self.result(), failure, self.result()],
            ) as run,
            self.assertRaisesRegex(release.ReleaseError, "push rejected"),
        ):
            release.create_and_push_tag(
                {"should_release": "true", "version": "0.2.0", "tag": "v0.2.0"}
            )

        self.assertEqual(
            run.call_args_list[-1],
            mock.call("git", "tag", "--delete", "v0.2.0", check=False),
        )


if __name__ == "__main__":
    unittest.main()
