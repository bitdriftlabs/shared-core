import os
import re
import tomllib
from pathlib import Path

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
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt
""".lstrip()

LEGACY_POLYFORM_RUST_HEADER = """
// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt
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

extensions_to_check = ('.rs', '.toml')

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
    normalized_path = file_path.lstrip('./')
    for pattern in exclude_patterns:
        if pattern.findall(normalized_path):
            return True

    for dir in exclude_dirs:
        if normalized_path.startswith(dir.lstrip('./')):
            return True

    return False


def package_manifests() -> dict[Path, str]:
    packages = {}
    for root, _, files in os.walk('.'):
        if 'Cargo.toml' not in files:
            continue
        manifest_path = Path(root) / 'Cargo.toml'
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


def check_manifest(manifest_path: Path, package_name: str, apache: set[str]) -> None:
    print(f'Checking {manifest_path}')
    with manifest_path.open('rb') as manifest:
        package = tomllib.load(manifest)['package']

    if package_name in apache:
        if package.get('license') != 'Apache-2.0' or 'license-file' in package:
            raise RuntimeError(f'{manifest_path} must declare license = "Apache-2.0"')
    elif package.get('license-file') != '../LICENSE.polyform' or 'license' in package:
        raise RuntimeError(f'{manifest_path} must declare license-file = "../LICENSE.polyform"')


def package_for_file(file_path: Path, packages: dict[Path, str]) -> str | None:
    for parent in (file_path.parent, *file_path.parents):
        package_name = packages.get(parent / 'Cargo.toml')
        if package_name is not None:
            return package_name
    return None


def check_file(file_path: str, packages: dict[Path, str], apache: set[str]):
    if is_excluded(file_path):
        return

    _, ext = os.path.splitext(file_path)
    if not ext in extensions_to_check:
        return

    package_name = package_for_file(Path(file_path), packages)
    if package_name is None or ext != '.rs':
        return

    with open(file_path) as file:
        content = file.read()

    expected_header = APACHE_RUST_HEADER if package_name in apache else POLYFORM_RUST_HEADER
    original_content = content

    while True:
        for header in (APACHE_RUST_HEADER, POLYFORM_RUST_HEADER, LEGACY_POLYFORM_RUST_HEADER):
            if content.startswith(header):
                content = content[len(header):].lstrip('\n')
                break
        else:
            break

    print(f'Checking {file_path}')
    if original_content != expected_header + '\n' + content:
        with open(file_path, 'w') as file:
            file.write(expected_header + '\n' + content)


def iterate_over_files():
    packages = package_manifests()
    apache = apache_packages()
    package_names = set(packages.values())
    undocumented = apache - package_names
    if undocumented:
        raise RuntimeError(f'LICENSES.md names unknown packages: {sorted(undocumented)}')

    for manifest_path, package_name in packages.items():
        check_manifest(manifest_path, package_name, apache)

    for root, _, files in os.walk('.'):
        for file in files:
            file_path = os.path.join(root, file)
            check_file(file_path, packages, apache)


# Run the script
iterate_over_files()
