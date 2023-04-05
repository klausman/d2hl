package main

import (
	"flag"
	"fmt"
	"os"

	log "github.com/inconshreveable/log15"
	"github.com/kormat/fmt15"
)

var logLevel = flag.String("log.level", "info", "Logging level (one of debug, info, warn, error, crit).")

func logSetup(timefmt string, doFlagParse bool) error {
	if doFlagParse {
		flag.Parse()
	}
	fmt15.TimeFmt = "15:04:05.000"
	if timefmt != "" {
		fmt15.TimeFmt = timefmt
	}
	logLvl, err := log.LvlFromString(*logLevel)
	if err != nil {
		return fmt.Errorf("cannot parse log level %s: %w", *logLevel, err)
	}
	handler := log.LvlFilterHandler(logLvl, log.StreamHandler(os.Stderr, fmt15.Fmt15Format(fmt15.ColorMap)))
	log.Root().SetHandler(handler)
	return nil
}
