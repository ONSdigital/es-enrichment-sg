#!/usr/bin/env bash

cd enrichment-repository
serverless plugin install --name serverless-latest-layer-version
echo Destroying serverless bundle...
serverless remove --verbose;
