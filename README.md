# hocc64-bskiplist

A concurrent B-SkipList implemented in C, inspired by the algorithm described in:

> Yicong Luo et al., *"[Bridging Cache-Friendliness and Concurrency: A Locality-Optimized In-Memory B-Skiplist]"*, ICPP 2025
> https://doi.org/10.1145/3754598.3754655
> Original C++ implementation: https://github.com/Ratbuyer/bskip_artifact


This implementation extends the original algorithm with a hybrid synchronisation
scheme: an optimistic lock-free traversal phase using optimistic version
validation, combined with pessimistic hand-over-hand locking during structural
modifications.

## Build

See [test/README.md](test/README.md) for how to run the correctness tests and YCSB driver.

## License

Apache License 2.0.

The algorithm is based on the work of Yicong Luo et al. (Apache 2.0).