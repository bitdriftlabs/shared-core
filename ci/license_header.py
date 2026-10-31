import argparse
import hashlib
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

STRICT_URL = 'https://polyformproject.org/licenses/strict/1.0.0'
STRICT_LICENSE_SHA256 = '90f9e6b7d79f381706abd79e5302039ca09856922a2cdc9bc91087fd1535581e'

APACHE_RUST_HEADER = """
// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
""".lstrip()

POLYFORM_RUST_HEADER = """
// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/licenses/strict/1.0.0
""".lstrip()

exclude_dirs = (
    './.git',
    './bd-proto/src/protos/',
    './fuzz/corpus/',
    './proto/',
    './target/',
    './thirdparty/',
)

exclude_patterns = (
    re.compile('generated'),
)

extensions_to_check = ('.rs', '.h', '.c', '.cc', '.cpp', '.sh', '.py')

extra_header_files = {
    'bd-workflow-bench/criterion.sh',
    'bd-workflow-bench/json_path.sh',
    'bd-workflow-bench/profile.sh',
    'scripts/setup-orbstack-linux-vm.sh',
    'ci/license_header_test.py',
}

LOCKED_APACHE_PACKAGES = {
    'bd-backoff',
    'bd-grpc',
    'bd-grpc-codec',
    'bd-log',
    'bd-log-util',
    'bd-panic',
    'bd-pgv',
    'bd-rt',
    'bd-runtime-config',
    'bd-server-stats',
    'bd-shutdown',
    'bd-stats-common',
    'bd-test-helpers-core',
    'bd-time',
    'bd-workspace-hack',
}


def is_excluded(file_path: str) -> bool:
    normalized_path = Path(file_path).as_posix().removeprefix('./')
    for pattern in exclude_patterns:
        if pattern.findall(normalized_path):
            return True

    for dir in exclude_dirs:
        if normalized_path.startswith(dir.removeprefix('./')):
            return True

    return False


def package_manifests() -> dict[Path, str]:
    packages = {}
    for manifest_path in iterate_over_files():
        if manifest_path.name != 'Cargo.toml':
            continue
        if manifest_path == Path('Cargo.toml') or is_excluded(str(manifest_path)):
            continue
        with manifest_path.open('rb') as manifest:
            package = tomllib.load(manifest).get('package')
        if package is not None:
            packages[manifest_path] = package['name']
    return packages


def apache_packages() -> set[str]:
    contents = Path('LICENSES.md').read_text()
    documented = set(re.findall(r'^\| `([^`]+)` \| Apache-2\.0 \|$', contents, re.MULTILINE))
    added = documented - LOCKED_APACHE_PACKAGES
    if added:
        raise RuntimeError(
            'New Apache packages require an explicit license discussion before they can be '
            f'allowlisted: {sorted(added)}. After that discussion, update '
            'LOCKED_APACHE_PACKAGES in ci/license_header.py.'
        )

    missing = LOCKED_APACHE_PACKAGES - documented
    if missing:
        raise RuntimeError(f'LICENSES.md must document locked Apache packages: {sorted(missing)}')

    return LOCKED_APACHE_PACKAGES


def check_manifest(manifest_path: Path, package_name: str, apache: set[str]) -> bool:
    print(f'Checking {manifest_path}')
    with manifest_path.open('rb') as manifest:
        package = tomllib.load(manifest)['package']

    if package_name in apache:
        if package.get('license') != 'Apache-2.0' or 'license-file' in package:
            print(f'{manifest_path} must declare license = "Apache-2.0"', file=sys.stderr)
            return False
    elif package.get('license-file') != '../LICENSE.polyform' or 'license' in package:
        print(f'{manifest_path} must declare license-file = "../LICENSE.polyform"', file=sys.stderr)
        return False
    elif (manifest_path.parent / package['license-file']).resolve() != Path('LICENSE.polyform').resolve():
        print(f'{manifest_path} must resolve to the root LICENSE.polyform', file=sys.stderr)
        return False
    return True


def package_for_file(file_path: Path, packages: dict[Path, str]) -> str | None:
    for parent in (file_path.parent, *file_path.parents):
        package_name = packages.get(parent / 'Cargo.toml')
        if package_name is not None:
            return package_name
    return None


def is_generated_binding(file_path: Path) -> bool:
    return file_path.as_posix().startswith((
        'bd-proto/src/protos/', 'bd-proto/src/flatbuffers/', 'bd-pgv/src/generated/',
        'bd-grpc/src/generated/', 'bd-script/src/report/generated/',
    ))


def check_file(
    file_path: Path, packages: dict[Path, str], apache: set[str], check: bool,
    generated: bool = False,
) -> bool:
    generated_binding = is_generated_binding(file_path)
    if is_excluded(str(file_path)) and not generated_binding:
        return True

    ext = file_path.suffix
    if not ext in extensions_to_check:
        return True

    package_name = package_for_file(file_path, packages)
    original_content = file_path.read_text()
    preamble = ''
    content = original_content.lstrip('\n')
    if content.startswith('#!') and ext in ('.sh', '.py'):
        first_line, _, content = content.partition('\n')
        preamble = first_line + '\n'
        if content.startswith('\n'):
            preamble += '\n'
            content = content[1:]

    comment = '#' if ext in ('.sh', '.py') else '//'
    banner = f"{comment} shared-core - bitdrift's common client/server libraries\n"
    if package_name is None and str(file_path) not in extra_header_files and not content.startswith(banner):
        return True

    def commented(header: str) -> str:
        return re.sub(r'^//', comment, header, flags=re.MULTILINE)

    expected_header = commented(APACHE_RUST_HEADER if package_name in apache else POLYFORM_RUST_HEADER)
    if generated_binding and not (check or generated):
        return True
    for match in re.finditer(re.escape(banner), original_content):
        block = original_content[match.start():].partition('\n\n')[0]
        if not original_content.startswith(expected_header, match.start()) and re.search(
            rf'^{re.escape(comment)}[^\n]*licens', block, flags=re.MULTILINE | re.IGNORECASE,
        ):
            print(f'Unrecognized license header: {file_path}', file=sys.stderr)
            return False
    if generated_binding:
        return True

    while content.startswith(expected_header):
        content = content[len(expected_header):].lstrip('\n')

    if not preamble and content.startswith('#!') and ext in ('.sh', '.py'):
        first_line, _, content = content.partition('\n')
        preamble = first_line + '\n'
        if content.startswith('\n'):
            preamble += '\n'
            content = content[1:]
    print(f'Checking {file_path}')
    expected_content = preamble + expected_header + '\n' + content
    if original_content != expected_content:
        if check:
            print(f'Incorrect license header: {file_path}', file=sys.stderr)
            return False
        file_path.write_text(expected_content)
    return True


def iterate_over_files():
    files = subprocess.check_output(
        ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'])
    for file_path in sorted(set(os.fsdecode(files).split('\0')) - {''}):
        path = Path(file_path)
        if path.is_file() and (not is_excluded(file_path) or is_generated_binding(path)):
            yield path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true', help='Check without modifying files')
    parser.add_argument(
        '--generated', action='store_true',
        help='Validate existing first-party binding headers after make protos')
    parser.add_argument('files', nargs='*')
    args = parser.parse_args()
    try:
        packages = package_manifests()
        apache = apache_packages()
        undocumented = apache - set(packages.values())
        if undocumented:
            raise RuntimeError(f'LICENSES.md names unknown packages: {sorted(undocumented)}')
        license_text = Path('LICENSE.polyform').read_bytes()
        if hashlib.sha256(license_text).hexdigest() != STRICT_LICENSE_SHA256:
            raise RuntimeError('LICENSE.polyform must contain PolyForm Strict License 1.0.0')
        valid = True
        for manifest_path, package_name in packages.items():
            valid = check_manifest(manifest_path, package_name, apache) and valid
        paths = (Path(os.path.relpath(path)) for path in args.files) if args.files else iterate_over_files()
        for path in paths:
            valid = check_file(path, packages, apache, args.check, args.generated) and valid
        return 0 if valid else 1
    except (OSError, RuntimeError, tomllib.TOMLDecodeError) as error:
        print(error, file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
