# GOBIGTABLEPERF

A test for [fetching](https://godoc.org/cloud.google.com/go/bigtable#Table.ReadRows)
performance in [BigTable](https://cloud.google.com/bigtable/docs/) via
[Google's SDK](https://godoc.org/cloud.google.com/go/bigtable).

Fetching from BigTable is the only thing being tested in this application. There
are some bulk writes and some admin operations, but those are not timed.

## Usage

| Global Parameters                                                              ||
| --------------------------- | ------------------------------------------------- |
| `--project value, -p value`  | Google Cloud project                             |
| `--instance value, -i value` | BigTable instance name                           |
| `--table value, -t value`    | BigTable table name (default: "gobigtable_test") |

### Command: `create-data`
Create the table with base data for the tests. This will create `count` rows of
random data of size `data-size`. The name of the table is set in the global
`table` value. If there is already a table of the same name, the command will
delete that table and make a new one. This operation is fairly slow, which is
why it has been broken out into its own command.

The table will have a single column family, `data`, with a single column,
`value` containing random bytes.

| Parameters                                                                        ||
| ------------------- | ------------------------------------------------------------ |
| `--count value`     | Number of entries to put into the test table (default: 5000) |
| `--data-size value` | Size of test data values, in bytes (default: 2048)           |

#### example
```bash
./gobigtableperf \
    --project bluecore-qa \
    --instance dangertest \
    create-data \
    --data-size 128 \
    --count 50000
```

### Command: `delete-data`
Destroy the table with base data for the tests. Note that this command does not
know if the table was created by this tool and will delete any table.

#### example
```bash
./gobigtableperf \
    --project bluecore-qa \
    --instance dangertest \
    delete-data
```

### Command: `concurrency`
Test various key counts at various levels of concurrency. The 50,000 test keys
are cached locally and the keys for each fetch are randomly selected from them.
The output is the median time of `--cycles` fetches for the given number of keys
in milliseconds. Concurrency goes up in powers of two times the minimum.

The 1-key line is repeated to allow the client a warm-up period.

| Parameters                                                              ||
| ----------------- | ---------------------------------------------------- |
|`--cycles value`   | Number of attempts for each key count (default: 100) |
|`--min-conc value` | Minimum concurrency (default: 1)                     |
|`--max-conc value` | Maximum concurrency (default: 16)                    |

#### Example
```bash
./gobigtableperf \
    --project bluecore-qa \
    --instance dangertest \
    concurrency
```

```
Holding 50000 keys
keys    c=1     c=2     c=4     c=8     c=16
1       0.096   0.132   0.151   0.182   0.175
1       0.236   0.217   0.195   0.186   0.177
5       0.194   0.097   0.060   0.070   0.184
10      0.235   0.113   0.067   0.141   0.184
15      0.219   0.181   0.104   0.192   0.089
20      0.121   0.140   0.089   0.229   0.225
25      0.279   0.269   0.230   0.249   0.241
30      0.292   0.219   0.098   0.233   0.266
35      0.279   0.286   0.100   0.241   0.264
40      0.292   0.214   0.153   0.316   0.326
45      0.363   0.157   0.122   0.134   0.327
50      0.338   0.234   0.218   0.338   0.349
55      0.306   0.328   0.288   0.358   0.327
60      0.369   0.369   0.139   0.297   0.337
65      0.381   0.197   0.141   0.348   0.352
70      0.351   0.393   0.366   0.392   0.376
75      0.375   0.348   0.188   0.382   0.380
80      0.407   0.436   0.384   0.429   0.409
85      0.393   0.242   0.173   0.205   0.418
90      0.435   0.399   0.194   0.428   0.455
95      0.429   0.357   0.298   0.428   0.434
100     0.403   0.408   0.191   0.452   0.476
Deleted 50000 test keys
```

### Command: `scatter`
Output key-count vs. time points, optionally plotting. The goal of this test is
to show the performance of the system under the load of a particular level of
concurrency.

Using the `--gnuplot` option will produce a script to generate a
PNG with linear trendline. Note that the `--gnuplot-extra` parameter will put
any custom GnuPlot commands into the output. This can be used to create custom
axis labels, titles, output formats, etc. This application does not check the
validity of those commands, so handle with care.

| Parameters                                                                        ||
| ---------------------- | --------------------------------------------------------- |
|`--concurrency value`   | Concurrency (default: 1)                                  |
|`--cycles value`        | Number of attempts for each key count (default: 100)      |
|`--min-keys value`      | Minimum number of keys to fetch in a cycle (default: 1)   |
|`--max-keys value`      | Maximum number of keys to fetch in a cycle (default: 100) |
|`--gnuplot`             | Output GnuPlot script for scatter                         |
|`--gnuplot-extra value` | Inject additional commands into the gnuplot render        |

 #### Example

 Running against localhost 10,000 times, producing a PNG graph.

 ```bash
 ./gobigtableperf \
     --project bluecore-qa \
     --instance dangertest \
     scatter \
     --cycles 1000 \
     --gnuplot | gnuplot > data.png
 ```
 ![example.png](example.png)
