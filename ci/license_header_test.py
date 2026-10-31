# shared-core - bitdrift's common client/server libraries
# Copyright Bitdrift, Inc. All rights reserved.
#
# Use of this source code is governed by a source available license that can be found in the
# LICENSE.polyform file or at:
# https://polyformproject.org/licenses/strict/1.0.0

import importlib.util
import pathlib
import subprocess
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).with_name('license_header.py').resolve()
REPO = SCRIPT.parent.parent
SPEC = importlib.util.spec_from_file_location('license_header', SCRIPT)
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


class LicenseHeaderTest(unittest.TestCase):
    def setUp(self):
        scratch = REPO / '.tmp'
        scratch.mkdir(exist_ok=True)
        directory = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(directory.cleanup)
        self.root = pathlib.Path(directory.name)
        subprocess.run(['git', 'init', '--quiet', str(self.root)], check=True)
        for name in ('LICENSE.polyform', 'LICENSES.md'):
            (self.root / name).write_bytes((REPO / name).read_bytes())
        for package in CHECKER.LOCKED_APACHE_PACKAGES:
            self.manifest(package, 'license = "Apache-2.0"')
        self.manifest('client', 'license-file = "../LICENSE.polyform"')

    def manifest(self, name, license):
        path = self.root / name / 'Cargo.toml'
        path.parent.mkdir(exist_ok=True)
        path.write_text(f'[package]\nname = "{name}"\n{license}\n')
        return path

    def run_checker(self, *args):
        return subprocess.run(
            ['python3', str(SCRIPT), *args], cwd=self.root, capture_output=True, text=True)

    def test_check_reports_all_errors_without_writes(self):
        paths = [self.root / 'client' / name for name in ('one.rs', 'two file.rs')]
        for path in paths:
            path.write_text('body\n')
        result = self.run_checker('--check')
        self.assertEqual(result.returncode, 1)
        for path in paths:
            self.assertIn(path.name, result.stderr)
            self.assertEqual(path.read_text(), 'body\n')

    def test_canonical_header_normalization_preserves_upstream_notice(self):
        path = self.root / 'client' / 'ffi.h'
        notice = '/* Copyright upstream. MIT License. */\nint entry();\n'
        notice = '\n'.join(CHECKER.POLYFORM_RUST_HEADER.splitlines()[:2]) + '\n\n' + notice
        path.write_text(CHECKER.POLYFORM_RUST_HEADER + '\n' + CHECKER.POLYFORM_RUST_HEADER + '\n' + notice)
        self.assertEqual(self.run_checker('--check').returncode, 1)
        self.assertEqual(self.run_checker().returncode, 0)
        expected = CHECKER.POLYFORM_RUST_HEADER + '\n' + notice
        self.assertEqual(path.read_text(), expected)
        self.assertEqual(self.run_checker().returncode, 0)
        self.assertEqual(path.read_text(), expected)
        self.assertEqual(self.run_checker('--check', str(path)).returncode, 0)

    def test_noncanonical_headers_are_rejected_without_writes(self):
        for extension in ('.rs', '.h', '.sh', '.py'):
            path = self.root / 'client' / ('source' + extension)
            header = CHECKER.POLYFORM_RUST_HEADER
            if extension in ('.sh', '.py'):
                header = '\n'.join(
                    line.replace('//', '#', 1) if line.startswith('//') else line
                    for line in header.split('\n'))
            incorrect = header.replace(CHECKER.STRICT_URL, 'https://example.invalid/obsolete-license')
            for original in (
                incorrect + '\nbody\n',
                header + '\n' + incorrect + '\nbody\n',
                header.replace('LICENSE.polyform', 'LICENSE') + '\nbody\n',
            ):
                for args in ((), ('--check',)):
                    with self.subTest(extension=extension, args=args, header=original):
                        path.write_text(original)
                        result = self.run_checker(*args, str(path))
                        self.assertEqual(result.returncode, 1, result.stdout)
                        self.assertIn('Unrecognized license header', result.stderr)
                        self.assertEqual(path.read_text(), original)
            path.unlink()

    def test_apache_headers_remain_apache(self):
        path = self.root / 'bd-log' / 'lib.rs'
        path.write_text('body\n')
        self.assertEqual(self.run_checker().returncode, 0)
        self.assertEqual(path.read_text(), CHECKER.APACHE_RUST_HEADER + '\nbody\n')
        incorrect = CHECKER.POLYFORM_RUST_HEADER + '\nbody\n'
        path.write_text(incorrect)
        for args in ((), ('--check',)):
            self.assertEqual(self.run_checker(*args, str(path)).returncode, 1)
            self.assertEqual(path.read_text(), incorrect)
        self.manifest('bd-log', 'license-file = "../LICENSE.polyform"')
        result = self.run_checker('--check')
        self.assertEqual(result.returncode, 1)
        self.assertIn('must declare license = "Apache-2.0"', result.stderr)

    def test_allowlist_cannot_change_silently(self):
        license_map = self.root / 'LICENSES.md'
        original = license_map.read_text()
        for changed in (
            original + '\n| `new-package` | Apache-2.0 |\n',
            original.replace('| `bd-log` | Apache-2.0 |\n', ''),
        ):
            license_map.write_text(changed)
            self.assertEqual(self.run_checker('--check').returncode, 1)

    def test_shell_preamble_and_unignored_files(self):
        path = self.root / 'client' / 'script.sh'
        path.write_text('#!/bin/bash\n\necho body\n')
        self.assertEqual(self.run_checker().returncode, 0)
        contents = path.read_text()
        self.assertTrue(contents.startswith('#!/bin/bash\n\n# shared-core'))
        self.assertIn('# ' + CHECKER.STRICT_URL, contents)
        self.assertTrue(contents.endswith('echo body\n'))
        self.assertEqual(self.run_checker('--check').returncode, 0)

    def test_generated_ignored_and_third_party_files_are_untouched(self):
        (self.root / '.gitignore').write_text('build/\n')
        for name in ('build/out.rs', 'thirdparty/code.rs', 'api/code.rs', 'client/generated/code.rs'):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('upstream\n')
        self.assertEqual(self.run_checker().returncode, 0)
        for name in ('build/out.rs', 'thirdparty/code.rs', 'api/code.rs', 'client/generated/code.rs'):
            self.assertEqual((self.root / name).read_text(), 'upstream\n')

    def test_invalid_license_metadata_and_text_fail(self):
        self.manifest('client', 'license-file = "LICENSE"')
        self.assertEqual(self.run_checker('--check').returncode, 1)
        self.manifest('client', 'license-file = "../LICENSE.polyform"\nlicense = "MIT"')
        self.assertEqual(self.run_checker('--check').returncode, 1)
        license_file = self.root / 'LICENSE.polyform'
        license_file.write_text(license_file.read_text() + '\nAdditional terms\n')
        self.assertEqual(self.run_checker('--check').returncode, 1)
        (self.root / 'LICENSE.polyform').unlink()
        self.assertEqual(self.run_checker('--check').returncode, 1)

    def test_generated_headers_are_validated_without_repairs(self):
        self.manifest('bd-proto', 'license-file = "../LICENSE.polyform"')
        directory = self.root / 'bd-proto/src/protos'
        directory.mkdir(parents=True)
        path = directory / 'owned.rs'
        original = CHECKER.POLYFORM_RUST_HEADER + '\nbody\n'
        path.write_text(original)
        upstream = directory / 'upstream.rs'
        upstream.write_text('// upstream generated code\n')
        self.assertEqual(self.run_checker().returncode, 0)
        self.assertEqual(path.read_text(), original)
        self.assertEqual(self.run_checker('--check').returncode, 0)
        self.assertEqual(self.run_checker('--generated').returncode, 0)
        self.assertEqual(path.read_text(), original)
        self.assertEqual(upstream.read_text(), '// upstream generated code\n')
        incorrect = CHECKER.POLYFORM_RUST_HEADER.replace(
            CHECKER.STRICT_URL, 'https://example.invalid/obsolete-license')
        for content in (
            incorrect + '\nbody\n',
            original + 'const HEADER: &str = r#"' + incorrect + '"#;\n',
        ):
            path.write_text(content)
            self.assertEqual(self.run_checker().returncode, 0)
            for arg in ('--check', '--generated'):
                result = self.run_checker(arg)
                self.assertEqual(result.returncode, 1)
                self.assertIn('Unrecognized license header', result.stderr)
                self.assertEqual(path.read_text(), content)
                self.assertEqual(upstream.read_text(), '// upstream generated code\n')


if __name__ == '__main__':
    unittest.main()
