#!/usr/bin/env bash

cd enrichment-deploy-repository
echo Destroying serverless bundle...
serverless remove --verbose;