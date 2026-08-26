package fault

import (
	"errors"
	"fmt"
)

var (
	ErrValidation    = errors.New("validation failed")
	ErrNotFound      = errors.New("not found")
	ErrConflict      = errors.New("conflict")
	ErrUnauthorized  = errors.New("unauthorized")
	ErrForbidden     = errors.New("forbidden")
	ErrInvalidState  = errors.New("invalid state transition")
	ErrVersion       = errors.New("version conflict")
	ErrLeaseLost     = errors.New("lease ownership lost")
	ErrExpired       = errors.New("expired")
	ErrAlreadyExists = errors.New("already exists")
)

type FieldError struct {
	Field   string
	Message string
}

func (e FieldError) Error() string {
	if e.Field == "" {
		return fmt.Sprintf("%v: %s", ErrValidation, e.Message)
	}
	return fmt.Sprintf("%v: %s: %s", ErrValidation, e.Field, e.Message)
}

func (e FieldError) Unwrap() error { return ErrValidation }

type OpError struct {
	Op  string
	Err error
}

func (e OpError) Error() string {
	if e.Op == "" {
		return e.Err.Error()
	}
	return e.Op + ": " + e.Err.Error()
}

func (e OpError) Unwrap() error { return e.Err }

func Wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	return OpError{Op: op, Err: err}
}

func Validation(field, message string) error {
	return FieldError{Field: field, Message: message}
}

func IsPublic(err error) bool {
	return errors.Is(err, ErrValidation) || errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrConflict) || errors.Is(err, ErrUnauthorized) ||
		errors.Is(err, ErrForbidden) || errors.Is(err, ErrInvalidState) ||
		errors.Is(err, ErrVersion) || errors.Is(err, ErrExpired) ||
		errors.Is(err, ErrAlreadyExists)
}
