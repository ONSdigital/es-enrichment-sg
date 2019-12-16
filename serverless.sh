#!/usr/bin/env bash

# Serverless deployment
cd enrichment-deploy-repository
echo Installing dependancies
npm install serverless-latest-layer-version --global
echo Packaging serverless bundle...
serverless package --package pkg
echo Deploying to AWS...
serverless deploy --verbose;