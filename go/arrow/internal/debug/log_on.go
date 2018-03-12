// +build debug

package debug

import (
	"log"
	"os"
)

var (
	debug = log.New(os.Stderr, "[D] ", log.LstdFlags)
)

func Log(msg interface{}) {
	debug.Output(1, getStringValue(msg))
}
