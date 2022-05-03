#!/bin/sh

docker build . -t db-sync:0.1
docker run -d --env-file prod.env --name db-sync0.1 db-sync:0.1
