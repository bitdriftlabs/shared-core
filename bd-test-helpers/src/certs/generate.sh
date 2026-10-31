#!/bin/bash

# shared-core - bitdrift's common client/server libraries
# Copyright Bitdrift, Inc. All rights reserved.
#
# Use of this source code is governed by a source available license that can be found in the
# LICENSE.polyform file or at:
# https://polyformproject.org/licenses/strict/1.0.0

# Uses https://github.com/square/certstrap/releases, which can be installed via GH or brew.
certstrap init --common-name "BitdriftAuth"
certstrap request-cert --common-name Bitdrift
certstrap sign Bitdrift --CA BitdriftAuth

mv out/Bitdrift.crt Bitdrift.crt
mv out/Bitdrift.key Bitdrift.key
