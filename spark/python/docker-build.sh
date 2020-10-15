#!/usr/bin/env bash

# invoke docker login first

# Build and tag the image:
docker build --tag museums:1.13 .

# tag latest image
docker tag museums:1.13 sniggel/museums:1.13

# Deploy the image to registry
docker push sniggel/museums:1.13
