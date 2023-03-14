#!/bin/sh

docker run -it --rm \
  -p 18888:8888 \
  -p 4040:4040 -p 4041:4041 -p 4042:4042 -p 4043:4043 \
  -v "$PWD":/home/jovyan/work \
  almondsh/almond:0.13.2-scala-2.12.17
