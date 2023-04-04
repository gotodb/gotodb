#!/bin/bash

echo "Building gotodb";

env GOOS=linux go build -o worker main.go

docker build -t zhenorzz/gotodb-worker  .

docker push zhenorzz/gotodb-worker:latest

