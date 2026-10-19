package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/eventsource"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/internal/sources"
	"github.com/sci-ndp/pelican-client-protocol/stomp-server-go/notificationserver"
)

const eventInterval = 5 * time.Second

func main() {
	port := flag.String("port", "8080", "port to listen on")
	debug := flag.Bool("debug", false, "log full frame contents (headers and body) for every frame")
	htpasswdFile := flag.String("htpasswd", "", "path to an htpasswd file; if absent, connections are unauthenticated")
	queueDB := flag.String("queue-db", "", "path to a SQLite database file for durable, on-disk client queues; if absent, queues are kept in memory only")
	pelicanEnabled := flag.Bool("pelican-enabled", false, "watch Pelican federation directories named by clients' own subscription parameters for new files; if set, this is the event source instead of the demo ticker")
	pelicanPollInterval := flag.Duration("pelican-poll-interval", time.Minute, "how often to poll watched Pelican directories")
	pelicanStateFile := flag.String("pelican-state-file", "", "path to a file recording each watched directory's last-observed listing, so a restart doesn't re-announce every existing file as new (required if --pelican-enabled is set)")
	pelicanBinary := flag.String("pelican-binary", "pelican", "path to the pelican CLI binary used to list watched directories")
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	// Exactly one event source is ever active: the Pelican directory watcher
	// if --pelican-enabled is set, else the persistent file-backed demo
	// source if TEST_EVENT_SEQ_FILE is set, else the plain in-memory demo
	// ticker. The Pelican watcher doesn't take a fixed directory: it derives
	// what to watch dynamically from each client's own subscription
	// parameters (see pelicanDirectoryFromParams), via QueueAdded/
	// QueueRemoved hooks messagequeue.App calls as clients (un)subscribe.
	var source eventsource.EventSource
	if *pelicanEnabled {
		if *pelicanStateFile == "" {
			logger.Error("--pelican-state-file is required when --pelican-enabled is set")
			os.Exit(1)
		}
		pelicanSource, err := sources.NewPelicanListingSource(*pelicanPollInterval, *pelicanStateFile, *pelicanBinary, logger)
		if err != nil {
			logger.Error("failed to start Pelican directory listing watcher", "error", err)
			os.Exit(1)
		}
		source = pelicanSource
		logger.Info("watching Pelican directories named by client subscription parameters", "poll-interval", *pelicanPollInterval, "state-file", *pelicanStateFile)
	} else if seqFile := os.Getenv("TEST_EVENT_SEQ_FILE"); seqFile != "" {
		seqSource, err := sources.NewSeqFileSource(seqFile, eventInterval, logger)
		if err != nil {
			logger.Error("failed to start persistent sequence event source", "path", seqFile, "error", err)
			os.Exit(1)
		}
		source = seqSource
		logger.Info("using persistent sequence event source", "TEST_EVENT_SEQ_FILE", seqFile)
	} else {
		source = sources.NewTickerSource(eventInterval)
		logger.Info("using in-memory demo ticker event source")
	}

	handler, err := notificationserver.New(notificationserver.NotificationServerArgs{
		EventSource:  source,
		Logger:       logger,
		HtpasswdFile: *htpasswdFile,
		QueueDB:      *queueDB,
	})
	if err != nil {
		logger.Error("failed to start notification server", "error", err)
		os.Exit(1)
	}

	addr := ":" + *port
	logger.Info("STOMP 1.2 WebSocket server listening", "addr", "ws://0.0.0.0"+addr)
	if err := http.ListenAndServe(addr, handler); err != nil {
		logger.Error("http server failed", "error", err)
		os.Exit(1)
	}
}
