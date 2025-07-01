# GZSET Benchmarking Quick Start

1. Start Valkey with your GZSET module loaded:
   `valkey-server --loadmodule /path/to/gzset.so`
2. Benchmark ZSET commands only:
   `./src/valkey-benchmark --ds zset -t zadd,zpopmin,zrange_100`
3. Benchmark GZSET commands only:
   `./src/valkey-benchmark --ds gzset -t zadd,zpopmin,zrange_100`
4. Compare both suites in one run:
   `./src/valkey-benchmark --ds both -t zadd`
5. Missing module will report `ERR unknown command 'GZADD'` and exit.
