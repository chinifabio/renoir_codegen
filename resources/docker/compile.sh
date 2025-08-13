#!/bin/bash

if [[ -z "${STORE_NAME}" ]]; then
    echo "Error: STORE_NAME variable is not set."
    exit 1
fi
if [[ -z "${TEAM}" ]]; then
    echo "Error: TEAM variable is not set."
    exit 1
fi
if [[ -z "${JOB_NAME}" ]]; then
    echo "Error: JOB_NAME variable is not set."
    exit 1
fi

if [[ -z "${MINIO_HOST}" ]]; then
    echo "Error: MINIO_HOST variable is not set."
    exit 1
fi
if [[ -z "${MINIO_ACCESS_KEY}" ]]; then
    echo "Error: MINIO_ACCESS_KEY variable is not set."
    exit 1
fi
if [[ -z "${MINIO_SECRET_KEY}" ]]; then
    echo "Error: MINIO_SECRET_KEY variable is not set."
    exit 1
fi

if [[ ! -s "target.json" ]]; then
    echo "target.json does not exist or is empty."
    exit 1
fi

cargo build --release
if [[ $? -ne 0 ]]; then
    echo "Cargo build failed."
    exit 1
fi

./mc alias set "${STORE_NAME}" "${MINIO_HOST}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}"
./mc mb "${STORE_NAME}/${TEAM}" || true
./mc cp target/release/docker "${STORE_NAME}/${TEAM}/$(date +%Y-%m-%dT%H:%M:%S)-${JOB_NAME}"