package proplyd

import (
	"context"
	"errors"
	"fmt"

	"github.com/c12s/proplyd/pkg/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrCacheUnavailable: the cache could not serve the request and there
	// was no fallback (write-through writes, or no Backend configured).
	ErrCacheUnavailable = errors.New("proplyd: cache unavailable")
	// ErrBackendUnavailable: the backing store could not be reached, neither
	// through the cache nor directly.
	ErrBackendUnavailable = errors.New("proplyd: backend unavailable")
	// ErrNamespaceConflict: the namespace exists with a different spec.
	ErrNamespaceConflict = errors.New("proplyd: namespace registered with a different spec")
	// ErrPrefixNotAllowed: the server does not allow the key prefix.
	ErrPrefixNotAllowed = errors.New("proplyd: key prefix not allowed")
	// ErrInvalidArgument: bad key, value or configuration.
	ErrInvalidArgument = errors.New("proplyd: invalid argument")
	// ErrClosed: the client or namespace was closed.
	ErrClosed = errors.New("proplyd: closed")
)

// mapErr turns a gRPC error into one of the package errors (wrapping the
// original).
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	switch protocol.Reason(err) {
	case protocol.ReasonNamespaceConflict:
		return fmt.Errorf("%w: %s", ErrNamespaceConflict, status.Convert(err).Message())
	case protocol.ReasonPrefixNotAllowed:
		return fmt.Errorf("%w: %s", ErrPrefixNotAllowed, status.Convert(err).Message())
	case protocol.ReasonInvalidArgument:
		return fmt.Errorf("%w: %s", ErrInvalidArgument, status.Convert(err).Message())
	case protocol.ReasonBackendUnavailable:
		return fmt.Errorf("%w: %w", ErrBackendUnavailable, err)
	case protocol.ReasonNamespaceNotFound:
		// Lost again right after re-registering (Redis flapping).
		return fmt.Errorf("%w: %w", ErrCacheUnavailable, err)
	}
	if cacheUnavailable(err) {
		return fmt.Errorf("%w: %w", ErrCacheUnavailable, err)
	}
	if status.Code(err) == codes.Canceled {
		return context.Canceled
	}
	return err
}
