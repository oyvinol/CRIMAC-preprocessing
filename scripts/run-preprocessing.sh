#!/bin/sh
echo "Running preprocessing..."
docker run -t --rm \
-v "/$(pwd)/../in:/datain" \
-v "/$(pwd)/../work:/workin" \
-v "/$(pwd)/../out:/dataout" \
--security-opt label=disable \
--env OUTPUT_TYPE=zarr \
--env MAIN_FREQ=38000 \
--env OUTPUT_NAME="test" \
--env WRITE_PNG=1 \
crimac-preprocessor