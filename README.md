# Loka Tech Assessment

## Introduction
door2door collects the live position of all vehicles in its fleet in real-time via a GPS sensor in each
vehicle. These vehicles run in operating periods that are managed by door2door’s operators. An API is
responsible for collecting information from the vehicles and place it on an S3 bucket, in raw format, to
be consumed.

The purpose of this challenge is to automate the build of a simple yet scalable data lake and data
warehouse that will enable our BI team to answer questions like:

*What is the average distance traveled by our vehicles during an operating period?*

## Data Architecture
