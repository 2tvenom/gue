package guex

import (
	"errors"
	"fmt"
	"time"
)

// ErrJobReschedule interface implementation allows errors to reschedule jobs in the individual basis.
type ErrJobReschedule interface {
	RescheduleJobAt() time.Time
}

var ErrDiscard = errors.New("error discard")

type errJobRescheduleIn struct {
	d time.Duration
	s string
}

// ErrRescheduleJobIn spawns an error that reschedules a job to run after some predefined duration.
func ErrRescheduleJobIn(d time.Duration, reason string) error {
	return errJobRescheduleIn{d: d, s: reason}
}

// Error implements error.Error()
func (e errJobRescheduleIn) Error() string {
	return fmt.Sprintf("rescheduling job in %s because %s", e.d.String(), e.s)
}

func (e errJobRescheduleIn) RescheduleJobAt() time.Time {
	return time.Now().UTC().Add(e.d)
}

type errJobRescheduleAt struct {
	t time.Time
	s string
}

// ErrRescheduleJobAt spawns an error that reschedules a job to run at some predefined time.
func ErrRescheduleJobAt(t time.Time, reason string) error {
	return errJobRescheduleAt{t: t, s: reason}
}

// Error implements error.Error()
func (e errJobRescheduleAt) Error() string {
	return fmt.Sprintf("rescheduling job at %q because %q", e.t.String(), e.s)
}

func (e errJobRescheduleAt) RescheduleJobAt() time.Time {
	return e.t
}
