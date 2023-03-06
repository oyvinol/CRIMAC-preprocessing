#!/bin/bash
cd ..
cd CRIMAC-preprocessing
docker image rm crimac-preprocessor && docker build -t crimac-preprocessor .
cd ..
cd CRIMAC-classifiers-unet
docker image rm crimac-classifier && docker build -t crimac-classifier .

