#!/bin/bash
pyro4-ns &
python frontend.py &
for _ in $(seq 1 "$1"); do
    python replica.py &
done

sleep 4
echo "Press something to kill jobs"
read -n1
kill -9 $(jobs -p)
