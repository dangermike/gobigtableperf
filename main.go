package main

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/urfave/cli"
	"golang.org/x/net/context"
)

func main() {
	app := cli.NewApp()
	app.Name = "gobigtableperf"
	app.Usage = "Multithreaded BigTable tester"
	cli.HelpFlag = cli.BoolFlag{
		Name:  "help, ?",
		Usage: "show help",
	}

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "project, p",
			Usage: "Google Cloud project",
		},
		cli.StringFlag{
			Name:  "instance, i",
			Usage: "BigTable instance name",
		},
		cli.StringFlag{
			Name:  "table, t",
			Value: "gobigtable_test",
			Usage: "BigTable table name",
		},
	}

	app.Commands = []cli.Command{
		cli.Command{
			Name:   "create-data",
			Usage:  "Create the table with base data for the tests",
			Action: createTableAction,
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  "count",
					Value: 5000,
					Usage: "Number of entries to put into the test table",
				},
				cli.IntFlag{
					Name:  "data-size",
					Value: 2048,
					Usage: "Size of test data values, in bytes",
				},
			},
		},
		cli.Command{
			Name:   "delete-data",
			Usage:  "Destroy the table with base data for the tests",
			Action: deleteTableAction,
		},
		cli.Command{
			Name:   "concurrency",
			Usage:  "Test various key counts at various levels of concurrency",
			Action: concurrencyAction,
			Flags: []cli.Flag{

				cli.IntFlag{
					Name:  "cycles",
					Value: 100,
					Usage: "Number of attempts for each key count",
				},
				cli.IntFlag{
					Name:  "min-conc",
					Value: 1,
					Usage: "Minimum concurrency",
				},
				cli.IntFlag{
					Name:  "max-conc",
					Value: 16,
					Usage: "Maximum concurrency",
				},
			},
		},
		cli.Command{
			Name:   "scatter",
			Usage:  "Output key-count vs. time points, optionally plotting",
			Action: scatterAction,
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  "concurrency",
					Value: 1,
					Usage: "Concurrency",
				},
				cli.IntFlag{
					Name:  "cycles",
					Value: 100,
					Usage: "Number of attempts for each key count",
				},
				cli.IntFlag{
					Name:  "min-keys",
					Value: 1,
					Usage: "Minimum number of keys to fetch in a cycle",
				},
				cli.IntFlag{
					Name:  "max-keys",
					Value: 100,
					Usage: "Maximum number of keys to fetch in a cycle",
				},
				cli.BoolFlag{
					Name:  "gnuplot",
					Usage: "Output GnuPlot script for scatter",
				},
				cli.StringSliceFlag{
					Name:  "gnuplot-extra",
					Usage: "Inject additional commands into the gnuplot render",
				},
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
}

func createTableAction(ctx *cli.Context) error {
	project := ctx.GlobalString("project")
	instance := ctx.GlobalString("instance")
	table := ctx.GlobalString("table")
	count := ctx.Int("count")
	dataSize := ctx.Int("data-size")
	con := context.Background()

	admin, err := bigtable.NewAdminClient(con, project, instance)
	if err != nil {
		fmt.Println("Failed to connect")
		return err
	}

	defer admin.Close()
	if terr := admin.DeleteTable(con, table); terr == nil {
		fmt.Println("Dropped old test table")
	}
	if terr := admin.CreateTable(con, table); terr != nil {
		fmt.Println("Failed to create test table")
		return terr
	}
	if terr := admin.CreateColumnFamily(con, table, "data"); terr != nil {
		return terr
	}
	admin.Close()

	client, err := bigtable.NewClient(con, project, instance)
	if err != nil {
		return err
	}
	defer client.Close()
	tab := client.Open(table)

	buf := make([]byte, dataSize)
	keyCnt := 0
	var batchKeys []string
	var mutations []*bigtable.Mutation

	for keyCnt < count {
		key := fmt.Sprintf("test_%05d", keyCnt)
		batchKeys = append(batchKeys, key)
		if _, err := rand.Read(buf); err != nil {
			return err
		}
		mut := bigtable.NewMutation()
		mut.Set("data", "value", bigtable.Now(), buf[:])
		mutations = append(mutations, mut)
		keyCnt++
		if len(mutations) > 5000 {
			errs, err := tab.ApplyBulk(con, batchKeys, mutations)
			if errs != nil {
				return errors.New("errors occurred")
			}
			if err != nil {
				return err
			}
			batchKeys = batchKeys[:0]
			mutations = mutations[:0]
		}
	}
	if len(mutations) > 0 {
		errs, err := tab.ApplyBulk(con, batchKeys, mutations)
		if errs != nil {
			return errors.New("errors occurred")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func deleteTableAction(ctx *cli.Context) error {
	project := ctx.GlobalString("project")
	instance := ctx.GlobalString("instance")
	table := ctx.GlobalString("table")
	con := context.Background()

	admin, err := bigtable.NewAdminClient(con, project, instance)
	if err != nil {
		fmt.Println("Failed to connect")
		return err
	}

	defer admin.Close()
	if terr := admin.DeleteTable(con, table); terr == nil {
		fmt.Println("Dropped test table")
	}
	return nil
}

func concurrencyAction(ctx *cli.Context) error {
	if ctx.Int("min-conc") < 1 {
		return errors.New("min-conc must be greater than zero")
	}
	if ctx.Int("min-conc") > ctx.Int("max-conc") {
		return errors.New("min-conc cannot exceed max-conc")
	}
	if ctx.Int("cycles") < 1 {
		return errors.New("cycles must be greater than 0")
	}

	con := context.Background()
	keys, err := getTestKeys(
		con,
		ctx.GlobalString("project"),
		ctx.GlobalString("instance"),
		ctx.GlobalString("table"),
	)
	if err != nil {
		fmt.Println("Failed to get keys")
		return err
	}

	fmt.Printf("Holding %d keys\n", len(keys))

	client, err := bigtable.NewClient(con, ctx.GlobalString("project"), ctx.GlobalString("instance"))
	if err != nil {
		fmt.Println("Failed to connect")
		return err
	}
	defer client.Close()

	counts := []int{1, 1}
	for i := 5; i <= 100; i += 5 {
		counts = append(counts, i)
	}
	concs := []int{}
	for c := ctx.Int("min-conc"); c <= ctx.Int("max-conc"); c <<= 1 {
		concs = append(concs, c)
	}
	fmt.Print("keys")
	for _, c := range concs {
		fmt.Printf("\tc=%d", c)
	}
	fmt.Println()
	for _, cnt := range counts {
		res := make([]int64, ctx.Int("cycles"))
		fmt.Print(cnt)
		for _, conc := range concs {
			var wg sync.WaitGroup
			indices := make(chan int) // indices into the result slice
			for t := 0; t < conc; t++ {
				wg.Add(1)
				go func() {
					table := client.Open(ctx.GlobalString("table"))
					defer wg.Done()
					mykeys := keys[:]         // copy for safety
					for ix := range indices { // ix is the index where the duration will be written
						shuffleKeys(mykeys)
						actCnt := 0
						start := time.Now()
						rerr := table.ReadRows(con, bigtable.RowList(mykeys[:cnt]), func(row bigtable.Row) bool {
							actCnt++
							return true
						},
							bigtable.RowFilter(bigtable.FamilyFilter("data")),
							bigtable.RowFilter(bigtable.ColumnFilter("value")),
						)
						dur := time.Since(start)
						if rerr != nil {
							panic(rerr)
						}
						if actCnt != cnt {
							fmt.Printf("row count mismatch: exp %d, act %d\n", cnt, actCnt)
						}
						res[ix] = dur.Nanoseconds()
					}
				}()
			}
			for c := 0; c < ctx.Int("cycles"); c++ {
				indices <- c
			}
			close(indices)
			wg.Wait()
			sortInt64(res)
			fmt.Printf("\t%0.3f", medianInt64(res)/1000000.0)
		}
		fmt.Println()
	}

	return err
}

type scatterItem struct {
	keys     int
	duration time.Duration
}

func scatterAction(ctx *cli.Context) error {
	if ctx.Int("min-keys") < 1 {
		return errors.New("min-keys must be greater than zero")
	}
	if ctx.Int("min-keys") > ctx.Int("max-keys") {
		return errors.New("min-keys cannot exceed max-keys")
	}
	if ctx.Int("cycles") < 1 {
		return errors.New("cycles must be greater than 0")
	}
	if ctx.Int("concurrency") < 1 {
		return errors.New("concurrency must be greater than 0")
	}

	con := context.Background()
	keys, err := getTestKeys(
		con,
		ctx.GlobalString("project"),
		ctx.GlobalString("instance"),
		ctx.GlobalString("table"),
	)
	if err != nil {
		fmt.Println("Failed to get keys")
		return err
	}

	if ctx.Bool("gnuplot") {
		fmt.Println("$DATABLOCK << EOD")
	}

	var wgWorkers sync.WaitGroup
	var wgWriter sync.WaitGroup
	indices := make(chan int) // indices into the result slice
	outchan := make(chan scatterItem)
	minKeys := ctx.Int("min-keys")
	maxKeys := ctx.Int("max-keys")
	keyRange := maxKeys - minKeys

	wgWriter.Add(1)
	go func() {
		defer wgWriter.Done()
		for si := range outchan {
			fmt.Printf("%d\t%0.3f\n", si.keys, float64(si.duration.Nanoseconds())/1000000.0)
		}
	}()
	for t := 0; t < ctx.Int("concurrency"); t++ {
		wgWorkers.Add(1)
		go func() {
			defer wgWorkers.Done()
			mykeys := keys[:] // copy for safety
			client, err := bigtable.NewClient(con, ctx.GlobalString("project"), ctx.GlobalString("instance"))
			if err != nil {
				fmt.Println("Failed to connect")
				panic(err)
			}
			defer client.Close()
			table := client.Open(ctx.GlobalString("table"))
			// warm up client
			table.ReadRows(con, bigtable.RowList(mykeys[:10]), func(row bigtable.Row) bool {
				return true
			},
				bigtable.RowFilter(bigtable.FamilyFilter("data")),
				bigtable.RowFilter(bigtable.ColumnFilter("value")),
			)
			for range indices {
				keyCnt := minKeys
				if keyRange > 0 {
					keyCnt += rand.Intn(keyRange)
				}
				shuffleKeys(mykeys)
				start := time.Now()
				rerr := table.ReadRows(con, bigtable.RowList(mykeys[:keyCnt]), func(row bigtable.Row) bool {
					return true
				},
					bigtable.RowFilter(bigtable.FamilyFilter("data")),
					bigtable.RowFilter(bigtable.ColumnFilter("value")),
				)
				dur := time.Since(start)
				if rerr != nil {
					panic(rerr)
				}
				outchan <- scatterItem{keyCnt, dur}
			}
		}()
	}
	for c := 0; c < ctx.Int("cycles"); c++ {
		indices <- c
	}
	close(indices)
	wgWorkers.Wait()
	close(outchan)
	wgWriter.Wait()

	if ctx.Bool("gnuplot") {
		fmt.Println("EOD")
		fmt.Println(`set fit nolog`)
		fmt.Println(`set fit quiet`)
		fmt.Println(`set term pngcairo size 1280, 1024 font "sans,16"`)
		fmt.Println(`set xlabel "key count"`)
		fmt.Println(`set ylabel "time (ms)"`)
		for _, line := range ctx.StringSlice("gnuplot-extra") {
			fmt.Println(line)
		}
		fmt.Println(`f(x) = a*x+b`)
		fmt.Println(`fit f(x) $DATABLOCK via a,b`)
		fmt.Printf("plot $DATABLOCK title \"mget (c=%d)\", f(x) with lines lw 3 title sprintf(\"y = %%0.6fx + %%0.6f\", a, b)\n", ctx.Int("concurrency"))
	}
	return nil
}

func getTestKeys(ctx context.Context, project, instance, table string) ([]string, error) {
	client, err := bigtable.NewClient(ctx, project, instance)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	tab := client.Open(table)

	var keys []string
	tab.ReadRows(
		ctx,
		bigtable.PrefixRange("test_"),
		func(row bigtable.Row) bool {
			keys = append(keys, row.Key())
			return true
		},
		bigtable.RowFilter(bigtable.FamilyFilter("data")),
		bigtable.RowFilter(bigtable.StripValueFilter()),
	)
	return keys, nil
}

func shuffleKeys(keys []string) {
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
}

func sortInt64(arr []int64) {
	sort.Slice(arr, func(i, j int) bool {
		return arr[i] < arr[j]
	})
}

func medianInt64(data []int64) float64 {
	l := len(data)
	if l%2 == 0 {
		return float64(data[l/2-1]+data[l/2+1]) / 2.0
	}
	return float64(data[l/2])
}
