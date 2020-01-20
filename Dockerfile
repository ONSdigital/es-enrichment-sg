FROM onsdigital/es-results-base:latest

COPY dev-requirements.txt /
RUN pip install -r /dev-requirements.txt
