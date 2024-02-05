package main

import (
	"io"
	"log/slog"

	"github.com/lmittmann/tint"
)

func logSetup(w io.Writer, level slog.Level, timefmt string, color bool) *slog.Logger {
	logger := slog.New(
		tint.NewHandler(w, &tint.Options{
			NoColor:    !color,
			TimeFormat: timefmt,
			Level:      level,
		}),
	)
	return logger
}
