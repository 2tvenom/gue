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
	s error
}

// ErrRescheduleJobIn spawns an error that reschedules a job to run after some predefined duration.
func ErrRescheduleJobIn(d time.Duration, reason error) error {
	return errJobRescheduleIn{d: d, s: reason}
}

// Error implements error.Error()
func (e errJobRescheduleIn) Error() string {
	return fmt.Sprintf("rescheduling job in %s because %s", e.d.String(), e.s.Error())
}

func (e errJobRescheduleIn) RescheduleJobAt() time.Time {
	return time.Now().UTC().Add(e.d)
}

func (e errJobRescheduleIn) Unwrap() error {
	return e.s
}

type errJobRescheduleAt struct {
	t time.Time
	s error
}

// ErrRescheduleJobAt spawns an error that reschedules a job to run at some predefined time.
func ErrRescheduleJobAt(t time.Time, reason error) error {
	return errJobRescheduleAt{t: t, s: reason}
}

// Error implements error.Error()
func (e errJobRescheduleAt) Error() string {
	return fmt.Sprintf("rescheduling job at %q because %s", e.t.String(), e.s.Error())
}

func (e errJobRescheduleAt) Unwrap() error {
	return e.s
}

func (e errJobRescheduleAt) RescheduleJobAt() time.Time {
	return e.t
}
