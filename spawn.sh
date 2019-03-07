#!/bin/bash
pyro4-ns &
python frontend.py &
# spawn 3 replicas
for _ in $(seq 1 3); do
    python replica.py &
done
