# rated

Distributed ratings system with replication via gossip.
Uses Pyro4.

```bash
$ pyro4-ns &
$ python frontend.py &
$ # run 10 replicas
$ for i in $(seq 10); do
>   python replica.py &
> done
```

## testing

```bash
$ tools/test_all
```

## todo

 - [x] lazy for creating ratings
 - [x] primary decided by frontend (least-id-wins)
 - [x] primary for forced updates
 - [ ] log compaction
