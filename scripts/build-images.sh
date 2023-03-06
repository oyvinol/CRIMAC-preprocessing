#!/bin/bash
cd ..
cd CRIMAC-preprocessing
docker build -t crimac-preprocessor .
cd ..
cd CRIMAC-classifiers-unet
docker build -t crimac-classifier .

