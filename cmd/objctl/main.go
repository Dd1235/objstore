// Command objctl is the command-line client for the object store.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Dd1235/objstore/internal/api"
	"github.com/Dd1235/objstore/internal/client"
	"github.com/Dd1235/objstore/internal/envflag"
)

const usage = `objctl: command-line client for the object store

Usage:
  objctl [-server URL] [-json] <command> [arguments]

Objects:
  put [-name NAME] [-type TYPE] [-meta key=value]... FILE   upload a file ("-" reads stdin; needs -name)
  get [-o PATH] ID           download, verifying size and SHA-256 (default: the stored name; "-" for stdout)
  info ID                    metadata with every chunk and where its copies are
  meta ID                    show custom metadata
  meta ID set key=value...   add or replace custom metadata keys
  meta ID unset key...       remove custom metadata keys
  ls [-limit N] [-offset N]  list objects, oldest first
  rm ID...                   delete objects

Cluster:
  workers                    worker states
  chunks WORKER              chunks stored on a worker
  status                     replication status: are all chunks at 2 copies?

The server defaults to $OBJSTORE_URL, else http://localhost:8090.
-json prints the controller's JSON response instead of a table.
`

type app struct {
	c      *client.Client
	json   bool
	stdout io.Writer
}

func main() {
	fs := flag.NewFlagSet("objctl", flag.ContinueOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	server := fs.String("server", envflag.String("OBJSTORE_URL", "http://localhost:8090"), "controller URL")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	if fs.NArg() == 0 {
		fs.Usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	a := &app{c: &client.Client{BaseURL: *server}, json: *asJSON, stdout: os.Stdout}
	err := a.run(ctx, fs.Arg(0), fs.Args()[1:])
	var ue usageError
	switch {
	case errors.As(err, &ue):
		fmt.Fprintf(os.Stderr, "objctl %s: %s\n\n", fs.Arg(0), ue)
		fs.Usage()
		os.Exit(2)
	case err != nil:
		fmt.Fprintln(os.Stderr, "objctl:", err)
		os.Exit(1)
	}
}

type usageError string

func (e usageError) Error() string { return string(e) }

func (a *app) run(ctx context.Context, cmd string, args []string) error {
	switch cmd {
	case "put":
		return a.put(ctx, args)
	case "get":
		return a.get(ctx, args)
	case "info":
		return a.info(ctx, args)
	case "meta":
		return a.meta(ctx, args)
	case "ls", "list":
		return a.list(ctx, args)
	case "rm", "delete":
		return a.remove(ctx, args)
	case "workers":
		return a.workers(ctx, args)
	case "chunks":
		return a.chunks(ctx, args)
	case "status":
		return a.status(ctx, args)
	case "help", "-h", "--help":
		fmt.Fprint(a.stdout, usage)
		return nil
	}
	return usageError("unknown command")
}

// parse parses flags that may appear before, between or after positional
// arguments, and returns the positional arguments.
func parse(fs *flag.FlagSet, args []string) ([]string, error) {
	fs.SetOutput(io.Discard)
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, usageError(err.Error())
		}
		if fs.NArg() == 0 {
			return pos, nil
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

func exactly(n int, args []string, what string) error {
	if len(args) != n {
		return usageError("expected " + what)
	}
	return nil
}

type metaFlag map[string]string

func (m metaFlag) String() string { return "" }
func (m metaFlag) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok || k == "" {
		return errors.New("metadata must be key=value")
	}
	m[k] = v
	return nil
}

func (a *app) put(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("put", flag.ContinueOnError)
	name := fs.String("name", "", "object name (default: the file name)")
	ctype := fs.String("type", "", "content type (default: from the name or the content)")
	md := metaFlag{}
	fs.Var(md, "meta", "custom metadata key=value (repeatable)")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if err := exactly(1, pos, "one FILE"); err != nil {
		return err
	}
	var r io.Reader
	if pos[0] == "-" {
		if *name == "" {
			return usageError("-name is required when reading stdin")
		}
		r = os.Stdin
	} else {
		f, err := os.Open(pos[0])
		if err != nil {
			return err
		}
		defer f.Close()
		r = f
		if *name == "" {
			*name = filepath.Base(pos[0])
		}
	}
	start := time.Now()
	info, err := a.c.Upload(ctx, *name, r, *ctype, md)
	if err != nil {
		return err
	}
	if a.json {
		return a.printJSON(info)
	}
	fmt.Fprintf(a.stdout, "stored %s (%s, %d chunks) in %s\n\n", info.ObjectID, size(info.Size), info.ChunkCount, time.Since(start).Round(time.Millisecond))
	a.printObject(info)
	return nil
}

func (a *app) get(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	out := fs.String("o", "", `output path (default: the stored name; "-" for stdout)`)
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if err := exactly(1, pos, "one object ID"); err != nil {
		return err
	}
	id := pos[0]
	if *out == "-" {
		_, err := a.c.Download(ctx, id, os.Stdout)
		return err
	}
	path := *out
	if path == "" {
		info, err := a.c.Metadata(ctx, id)
		if err != nil {
			return err
		}
		path = filepath.Base(info.Name)
		if path == "." || path == "/" || path == ".." {
			path = id
		}
	}
	// Write to a temporary file next to the target and rename it into place
	// only once the checksum has been verified.
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.part")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	start := time.Now()
	d, err := a.c.Download(ctx, id, tmp)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	if a.json {
		return a.printJSON(map[string]any{"object_id": id, "path": path, "size": d.Size, "sha256": d.SHA256, "verified": true})
	}
	fmt.Fprintf(a.stdout, "wrote %s (%s) in %s; sha256 %s verified\n", path, size(d.Size), time.Since(start).Round(time.Millisecond), d.SHA256)
	return nil
}

func (a *app) info(ctx context.Context, args []string) error {
	if err := exactly(1, args, "one object ID"); err != nil {
		return err
	}
	info, err := a.c.Metadata(ctx, args[0])
	if err != nil {
		return err
	}
	if a.json {
		return a.printJSON(info)
	}
	a.printObject(info)
	return nil
}

func (a *app) meta(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return usageError("expected an object ID")
	}
	id := args[0]
	var info api.ObjectInfo
	var err error
	switch {
	case len(args) == 1:
		info, err = a.c.Metadata(ctx, id)
	case args[1] == "set" && len(args) > 2:
		patch := map[string]*string{}
		for _, kv := range args[2:] {
			k, v, ok := strings.Cut(kv, "=")
			if !ok || k == "" {
				return usageError("set takes key=value pairs")
			}
			patch[k] = &v
		}
		info, err = a.c.PatchMetadata(ctx, id, patch)
	case args[1] == "unset" && len(args) > 2:
		patch := map[string]*string{}
		for _, k := range args[2:] {
			patch[k] = nil
		}
		info, err = a.c.PatchMetadata(ctx, id, patch)
	default:
		return usageError("expected: meta ID, meta ID set key=value..., or meta ID unset key...")
	}
	if err != nil {
		return err
	}
	if a.json {
		return a.printJSON(info.Metadata)
	}
	if len(info.Metadata) == 0 {
		fmt.Fprintln(a.stdout, "(no custom metadata)")
	}
	tw := a.table()
	for _, k := range sortedKeys(info.Metadata) {
		fmt.Fprintf(tw, "%s\t%s\n", k, info.Metadata[k])
	}
	tw.Flush()
	fmt.Fprintf(a.stdout, "(last modified %s)\n", info.ModifiedAt.Local().Format(time.DateTime))
	return nil
}

func (a *app) list(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("ls", flag.ContinueOnError)
	limit := fs.Int("limit", 0, "objects per page")
	offset := fs.Int("offset", 0, "objects to skip")
	if pos, err := parse(fs, args); err != nil {
		return err
	} else if len(pos) > 0 {
		return usageError("ls takes no arguments")
	}
	list, err := a.c.List(ctx, *limit, *offset)
	if err != nil {
		return err
	}
	if a.json {
		return a.printJSON(list)
	}
	tw := a.table()
	fmt.Fprintln(tw, "OBJECT ID\tNAME\tSIZE\tCHUNKS\tTYPE\tCREATED")
	for _, o := range list.Objects {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\n", o.ObjectID, o.Name, size(o.Size), o.ChunkCount, o.ContentType, o.CreatedAt.Local().Format(time.DateTime))
	}
	tw.Flush()
	fmt.Fprintf(a.stdout, "%d of %d objects", len(list.Objects), list.Total)
	if list.NextOffset != nil {
		fmt.Fprintf(a.stdout, "; more with -offset %d", *list.NextOffset)
	}
	fmt.Fprintln(a.stdout)
	return nil
}

func (a *app) remove(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return usageError("expected one or more object IDs")
	}
	for _, id := range args {
		if err := a.c.Delete(ctx, id); err != nil {
			return fmt.Errorf("%s: %w", id, err)
		}
		if !a.json {
			fmt.Fprintln(a.stdout, "deleted", id)
		}
	}
	return nil
}

func (a *app) workers(ctx context.Context, args []string) error {
	if err := exactly(0, args, "no arguments"); err != nil {
		return err
	}
	list, err := a.c.Workers(ctx)
	if err != nil {
		return err
	}
	if a.json {
		return a.printJSON(list)
	}
	tw := a.table()
	fmt.Fprintln(tw, "WORKER\tSTATE\tSINCE\tLAST HEARTBEAT\tCHUNKS\tUSED\tADDRESS")
	for _, w := range list.Workers {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s ago\t%d\t%s\t%s\n", w.WorkerID, state(w.State, w.Rejoining),
			w.StateSince.Local().Format(time.TimeOnly), ago(w.LastHeartbeat), w.ChunkCount, size(w.UsedBytes), w.Addr)
	}
	return tw.Flush()
}

func (a *app) chunks(ctx context.Context, args []string) error {
	if err := exactly(1, args, "one worker ID"); err != nil {
		return err
	}
	inv, err := a.c.WorkerChunks(ctx, args[0])
	if err != nil {
		return err
	}
	if a.json {
		return a.printJSON(inv)
	}
	tw := a.table()
	fmt.Fprintln(tw, "CHUNK ID\tSIZE\tSHA-256\tSTATE")
	for _, ch := range inv.Chunks {
		st := "ok"
		if ch.Corrupt {
			st = "CORRUPT"
		}
		fmt.Fprintf(tw, "%s\t%d\t%s…\t%s\n", ch.ChunkID, ch.Size, ch.SHA256[:min(16, len(ch.SHA256))], st)
	}
	tw.Flush()
	fmt.Fprintf(a.stdout, "%s holds %d chunks (%s)\n", inv.WorkerID, inv.ChunkCount, size(inv.UsedBytes))
	return nil
}

func (a *app) status(ctx context.Context, args []string) error {
	if err := exactly(0, args, "no arguments"); err != nil {
		return err
	}
	st, err := a.c.Replication(ctx)
	if err != nil {
		return err
	}
	if a.json {
		return a.printJSON(st)
	}
	verdict := "MAINTAINED"
	if !st.Maintained {
		verdict = "NOT MAINTAINED"
	}
	fmt.Fprintf(a.stdout, "replication factor %d: %s\n", st.ReplicationFactor, verdict)
	fmt.Fprintf(a.stdout, "%d objects, %d chunks: %d healthy, %d at risk, %d under-replicated, %d over-replicated, %d unavailable\n\n",
		st.Objects, st.Chunks, st.Healthy, st.AtRisk, st.UnderReplicated, st.OverReplicated, st.Unavailable)
	tw := a.table()
	fmt.Fprintln(tw, "WORKER\tSTATE\tCOPIES")
	for _, w := range st.Workers {
		note := ""
		if w.InventoryError != "" {
			note = "\t(unreachable)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%d%s\n", w.WorkerID, state(w.State, w.Rejoining), w.Copies, note)
	}
	tw.Flush()
	if len(st.Problems) > 0 {
		fmt.Fprintln(a.stdout)
		tw = a.table()
		fmt.Fprintln(tw, "CHUNK ID\tSTATUS\tCOPIES\tDOWN\tBAD")
		for _, p := range st.Problems {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", p.ChunkID, p.Status, list(p.Copies), list(p.Down), list(p.Bad))
		}
		tw.Flush()
	}
	if st.PendingActions > 0 {
		fmt.Fprintf(a.stdout, "\n%d repair steps pending\n", st.PendingActions)
	}
	if lr := st.LastRepair; lr != nil {
		fmt.Fprintf(a.stdout, "\nlast repair pass %s ago: %d copied, %d adopted, %d dropped, %d failed\n",
			ago(lr.At), lr.Copied, lr.Adopted, lr.Dropped, lr.Failed)
	}
	return nil
}

func (a *app) printObject(o api.ObjectInfo) {
	tw := a.table()
	fmt.Fprintf(tw, "object id\t%s\n", o.ObjectID)
	fmt.Fprintf(tw, "name\t%s\n", o.Name)
	fmt.Fprintf(tw, "size\t%d bytes (%s)\n", o.Size, size(o.Size))
	fmt.Fprintf(tw, "content type\t%s\n", o.ContentType)
	fmt.Fprintf(tw, "sha256\t%s\n", o.SHA256)
	fmt.Fprintf(tw, "created\t%s\n", o.CreatedAt.Local().Format(time.DateTime))
	fmt.Fprintf(tw, "modified\t%s\n", o.ModifiedAt.Local().Format(time.DateTime))
	fmt.Fprintf(tw, "chunks\t%d of up to %s\n", o.ChunkCount, size(int64(o.ChunkSize)))
	for i, k := range sortedKeys(o.Metadata) {
		label := ""
		if i == 0 {
			label = "metadata"
		}
		fmt.Fprintf(tw, "%s\t%s=%s\n", label, k, o.Metadata[k])
	}
	tw.Flush()
	if len(o.Chunks) == 0 {
		return
	}
	fmt.Fprintln(a.stdout)
	tw = a.table()
	fmt.Fprintln(tw, "INDEX\tCHUNK ID\tSIZE\tCOPIES ON")
	for _, ch := range o.Chunks {
		fmt.Fprintf(tw, "%d\t%s\t%d\t%s\n", ch.Index, ch.ChunkID, ch.Size, list(ch.Replicas))
	}
	tw.Flush()
}

func (a *app) table() *tabwriter.Writer { return tabwriter.NewWriter(a.stdout, 0, 0, 2, ' ', 0) }

func (a *app) printJSON(v any) error {
	enc := json.NewEncoder(a.stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func state(s string, rejoining bool) string {
	if rejoining {
		return s + " (rejoining)"
	}
	return s
}

func list(s []string) string {
	if len(s) == 0 {
		return "-"
	}
	return strings.Join(s, ", ")
}

func ago(t time.Time) string {
	return time.Since(t).Round(100 * time.Millisecond).String()
}

func size(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
