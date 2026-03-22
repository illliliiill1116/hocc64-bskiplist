# test

## Correctness

```bash
make DEBUG=1
./build/test_bsl
```

With ASAN:

```bash
make ASAN=1 DEBUG=1
./build/test_bsl
```

## YCSB

```bash
make
./build/ycsb <load_file> <run_file> <threads> <keys>
```

Example:

```bash
./build/ycsb /data/loada_unif_int.dat /data/txnsa_unif_int.dat 8 100000000
```

With latency collection:

```bash
make MEASURE_LATENCY=1
./build/ycsb /data/loada_unif_int.dat /data/txnsa_unif_int.dat 8 100000000
```