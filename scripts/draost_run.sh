#!/bin/bash

CONTAINER="opencadc/draost2caom2"

echo "Get the container"
docker pull ${CONTAINER} || exit $?

echo "Run container ${CONTAINER}"
docker run --rm --name draost_run -v ${PWD}:/usr/src/app/ ${CONTAINER} draost_run || exit $?

date
exit 0
