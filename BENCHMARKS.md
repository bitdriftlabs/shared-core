# Benchmarks

We use [Gungraun](https://github.com/gungraun/gungraun) to run benchmarks. This is a wrapper around
Valgrind's Callgrind tool which provides cycle accurate profiling of benchmark functions. This means
that the benchmarks only run on platforms that fully support Valgrind, which does not include Mac.
In order to run the benchmarks you will need to run them on Linux. An aarch64 Linux VM on Mac will
work just fine.

To view profiling data it is recommended to use
[KCacheGrind](https://kcachegrind.sourceforge.net/html/Home.html) which is a visual wrapper around
the Callgrind profiles.

In order to have proper source mappings when view the profiles, it is best to run KCacheGrind
directly on Linux. So if using a VM on Mac, you will need to setup X forwarding to send the display
info to the Mac. For an example of how to do this when using OrbStack see [this
issue](https://github.com/orbstack/orbstack/issues/139#issuecomment-1595364746). You should be able
to use any VM provider that supports X forwarding.

To run the benchmarks on linux use for example:

```
cargo bench -p bd-workflows
```

The callgrind output which can be opened in KCacheGrind will show up in:

```
ll target/gungraun/
```
