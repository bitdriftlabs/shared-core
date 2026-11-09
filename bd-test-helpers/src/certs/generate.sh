#!/bin/bash

# Uses https://github.com/square/certstrap/releases, which can be installed via GH or brew.
certstrap init --common-name "BitdriftAuth"
certstrap request-cert --common-name Bitdrift
certstrap sign Bitdrift --CA BitdriftAuth

mv out/Bitdrift.crt Bitdrift.crt
mv out/Bitdrift.key Bitdrift.key
