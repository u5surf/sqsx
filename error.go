package sqsx

import (
	"fmt"
)

type causer interface {
	Cause() error
}

type Error struct {
	cause error
	msg   string
}

func (e Error) Cause() error {
	return e.cause
}

func (e Error) String() string {
	return e.Error()
}

func (e Error) Error() string {
	if e.cause == nil {
		return fmt.Sprintf("sqsx: %s", e.msg)
	}
	return fmt.Sprintf("sqsx: %s\n\t%v", e.msg, e.cause)
}

func errorf(cause error, msg string, args ...interface{}) error {
	return &Error{
		cause: cause,
		msg:   fmt.Sprintf(msg, args...),
	}
}
