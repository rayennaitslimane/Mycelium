// Command keyring creates the ring files the gateway loads with -keyring (ADR 0011).
//
//	keyring gen -id N -out ring.key            one new KEK, with its secret key
//	keyring pub -in ring.key -out ring.pub     the same ring without secret keys
//
// A gateway given only a public ring can write but not read. To add a KEK later, put the
// new record first: cat new.key old.key > ring.key (the first KEK seals).
package main

import (
	"flag"
	"fmt"
	"os"

	"mycelium/core/seal"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "keyring:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: keyring gen|pub ... (see -h)")
	}
	fs := flag.NewFlagSet(args[0], flag.ExitOnError)
	id := fs.Uint("id", 0, "gen: KEK id (must be unique in the ring)")
	in := fs.String("in", "", "pub: ring file to read")
	out := fs.String("out", "", "file to create; it must not exist")
	fs.Parse(args[1:])
	if *out == "" {
		return fmt.Errorf("need -out")
	}

	var data []byte
	var err error
	switch args[0] {
	case "gen":
		data, err = seal.Generate(uint32(*id))
	case "pub":
		var file []byte
		if file, err = os.ReadFile(*in); err == nil {
			data, err = seal.PublicOnly(file)
			clear(file)
		}
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	if err != nil {
		return err
	}
	defer clear(data)

	f, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(*out)
	}
	return err
}
