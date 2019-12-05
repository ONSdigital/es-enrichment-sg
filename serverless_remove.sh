#!/usr/bin/env bash

cd enrichment-repository
echo Destroying serverless bundle...
serverless remove --verbose;
