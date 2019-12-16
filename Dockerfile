FROM python:3.7-slim

COPY dev-requirements.txt /
RUN apt-get update -y &&\
    apt-get install git -y
RUN pip install -r /dev-requirements.txt
RUN apt-get update -y &&\
    apt-get install curl -y &&\
    curl -sL https://deb.nodesource.com/setup_10.x | bash &&\
    apt-get install nodejs -y &&\
    node -v &&\
    apt-get -y install npm &&\
    npm -v &&\
    npm install -g serverless &&\
    npm install -g serverless-latest-layer-version &&\
    npm list | grep serverless-latest-layer-version