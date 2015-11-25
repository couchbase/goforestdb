package forestdb

import "C"
import "unsafe"

//export LogCallbackInternal
func LogCallbackInternal(errCode C.int, msg *C.char, ctx *C.char) {
	context := (*logContext)(unsafe.Pointer(ctx))
	(*context.callback)(context.name, int(errCode), C.GoString(msg), context.userCtx)
}

//export FatalErrorCallbackInternal
func FatalErrorCallbackInternal() {
	fatalErrorCallback()
}

// Logger interface
type Logger interface {
	// Warnings, logged by default.
	Warnf(format string, v ...interface{})
	// Errors, logged by default.
	Errorf(format string, v ...interface{})
	// Fatal errors. Will not terminate execution.
	Fatalf(format string, v ...interface{})
	// Informational messages.
	Infof(format string, v ...interface{})
	// Timing utility
	Debugf(format string, v ...interface{})
	// Program execution tracing. Not logged by default
	Tracef(format string, v ...interface{})
}

type Dummy struct {
}

func (*Dummy) Fatalf(_ string, _ ...interface{}) {
}

func (*Dummy) Errorf(_ string, _ ...interface{}) {
}

func (*Dummy) Warnf(_ string, _ ...interface{}) {
}

func (*Dummy) Infof(_ string, _ ...interface{}) {
}

func (*Dummy) Debugf(_ string, _ ...interface{}) {
}

func (*Dummy) Tracef(_ string, _ ...interface{}) {
}

// Logger to use
var Log Logger = &Dummy{}
