import os

# Define the header you want to check for and insert
rust_header = """
// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt
"""

headers = {
    '.rs': rust_header
}

exclude_dirs = (
    './.git',
    './bd-grpc/src/generated',
    './bd-pgv/src/generated',
    './bd-report-convert/src/generated',
    './bd-proto/',
    './fuzz/corpus/',
    './proto/',
    './target/',
    './thirdparty/',
)

extensions_to_check = ('.rs', '.toml')


def check_file(file_path):
    for dir in exclude_dirs:
        if file_path.startswith(dir):
            return

    _, ext = os.path.splitext(file_path)
    if not ext in extensions_to_check:
        return

    print(f'Checking {file_path}')
    with open(file_path, 'r+') as file:
        content = file.read()

        if (file_path.endswith('Cargo.toml') and
            not file_path == './Cargo.toml' and
                not 'license-file = "../LICENSE"' in content):
            raise Exception(
                f'license-file = "../LICENSE" not found in {file_path}')

        header = headers.get(ext)
        if not header:
            return
        header = header.lstrip()

        if not content.startswith(header):
            file.seek(0, 0)
            file.write(header + '\n' + content)


def iterate_over_files():
    for root, _, files in os.walk('.'):
        for file in files:
            file_path = os.path.join(root, file)
            check_file(file_path)


# Run the script
iterate_over_files()
