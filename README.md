rated
=====

Distributed ratings system with replication via gossip.
Uses Pyro4.

```bash
$ pyro4-nsc &
$ python frontend.py &
$ # run 10 replicas
$ for i in $(seq 10); do
>   python replica.py &
> done
```
