# Ecosia Data Engineering Assignment - Riccardo Frontini

## Prerequisites

In order to run the python program, Docker is required (Docker version 20.10.7 used for development)

## Execution

In order to build the container, open a terminal in the root directory and execute the following:
```bash
docker build -t riccardofrontini_ecosia .
```

To run the job, execute the following in a UNIX-compatible shell terminal:
```bash
docker run --rm -it -v $PWD/out:out \
    -e AWS_ACCESS_KEY_ID="your AWS Key ID" \
    -e AWS_SECRET_ACCESS_KEY="your AWS Secret access key" \
    -e AWS_S3_BUCKET="your bucket name" \
    riccardofrontini_ecosia
```
