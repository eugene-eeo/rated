#!/bin/bash
num="$1"
if [ ! "$num" ]; then
    num="3"
fi

pyro4-ns &
python frontend.py &
for _ in $(seq 1 "$num"); do
    python replica.py &
done

sleep 4
echo "Press something to kill jobs"
read -n1
kill -9 $(jobs -p)
