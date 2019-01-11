ARG DOCKER_REPO=pwcasdnmp.azurecr.io
ARG NUCLIO_BASE_IMAGE=library/python-processor
ARG NUCLIO_BASE_IMAGE_TAG=alpine-3.6

# From the base image
FROM ${DOCKER_REPO}/${NUCLIO_BASE_IMAGE}:${NUCLIO_BASE_IMAGE_TAG}

ADD requirements.txt .

RUN apk --no-cache add \
    	gcc \
    	librdkafka-dev \ 
    	musl-dev && \
    pip install -r requirements.txt && \
    apk del gcc musl-dev

ADD main.py /opt/nuclio
