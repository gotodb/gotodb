#!/bin/bash

echo "Building gotodb";

env GOOS=linux go build -o coordinator main.go

docker build -t zhenorzz/gotodb  .

#docker push zhenorzz/gotodb:latest

