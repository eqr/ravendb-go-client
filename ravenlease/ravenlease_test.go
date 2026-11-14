package ravenlease

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeStore struct {
	mu        sync.Mutex
	lease     *Lease[string]
	index     int64
	getErr    error
	putErr    error
	deleteErr error
	puts      int
}

func (s *fakeStore) Get(context.Context) (*Lease[string], int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getErr != nil {
		return nil, 0, s.getErr
	}
	if s.lease == nil {
		return nil, 0, nil
	}
	lease := *s.lease
	return &lease, s.index, nil
}

func (s *fakeStore) Put(_ context.Context, lease Lease[string], expected int64) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.putErr != nil {
		return 0, false, s.putErr
	}
	current := int64(0)
	if s.lease != nil {
		current = s.index
	}
	if expected != current {
		return current, false, nil
	}
	s.index++
	s.lease = &lease
	s.puts++
	return s.index, true, nil
}

func (s *fakeStore) Delete(_ context.Context, expected int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteErr != nil {
		return false, s.deleteErr
	}
	if s.lease == nil || s.index != expected {
		return false, nil
	}
	s.lease = nil
	s.index++
	return true, nil
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func newManager(store Store[string], holder string, clock *fakeClock) *Manager[string] {
	m := NewManager(store, holder, Options{})
	if clock != nil {
		m.now = clock.Now
	}
	return m
}

func TestAcquireAndRenew(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	store := &fakeStore{}
	m := newManager(store, "one", clock)
	m.SetData("segment-a")
	m.reconcile(context.Background())
	if !m.IsLeader() {
		t.Fatal("manager did not acquire missing lease")
	}
	first, ok := m.CurrentLease()
	if !ok || first.Data != "segment-a" {
		t.Fatalf("unexpected acquired lease: %+v, %v", first, ok)
	}
	firstExpiry := m.localExpiry
	m.SetData("segment-b")
	if got, _ := m.CurrentLease(); got.Data != "segment-b" {
		t.Fatalf("local data was not updated immediately: %q", got.Data)
	}
	clock.Advance(time.Second)
	m.reconcile(context.Background())
	second, ok := m.CurrentLease()
	if !ok || second.Data != "segment-b" || !m.localExpiry.After(firstExpiry) {
		t.Fatalf("lease was not renewed: %+v, %v", second, ok)
	}
	if store.puts != 2 {
		t.Fatalf("expected acquire and renew writes, got %d", store.puts)
	}
}

func TestActiveOtherLeaseRejected(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	store := &fakeStore{lease: &Lease[string]{Holder: "other", Data: "active"}, index: 4}
	m := newManager(store, "one", clock)
	m.reconcile(context.Background())
	if m.IsLeader() {
		t.Fatal("manager acquired another holder's active lease")
	}
	lease, ok := m.CurrentLease()
	if !ok || lease.Holder != "other" || lease.Data != "active" {
		t.Fatalf("follower snapshot not retained: %+v, %v", lease, ok)
	}
	if store.puts != 0 {
		t.Fatal("active lease was overwritten")
	}
}

func TestFollowerClockSkewDoesNotCausePrematureTakeover(t *testing.T) {
	base := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for _, offset := range []time.Duration{48 * time.Hour, -48 * time.Hour} {
		t.Run(offset.String(), func(t *testing.T) {
			store := &fakeStore{}
			leaderClock := &fakeClock{now: base}
			leader := newManager(store, "leader", leaderClock)
			leader.reconcile(context.Background())
			followerClock := &fakeClock{now: base.Add(offset)}
			follower := newManager(store, "follower", followerClock)
			follower.reconcile(context.Background())

			for range 3 {
				for range 3 {
					leaderClock.Advance(DefaultRenewInterval)
					leader.reconcile(context.Background())
				}
				followerClock.Advance(DefaultTTL - time.Second)
				follower.reconcile(context.Background())
				if follower.IsLeader() || store.lease.Holder != "leader" {
					t.Fatalf("follower took over with changing index at clock offset %s", offset)
				}
			}
		})
	}
}

func TestFollowerTakesOverAfterIndexUnchangedForTTL(t *testing.T) {
	store := &fakeStore{}
	leaderClock := &fakeClock{now: time.Unix(1000, 0)}
	leader := newManager(store, "leader", leaderClock)
	leader.SetData("live.mp3")
	leader.reconcile(context.Background())

	followerClock := &fakeClock{now: time.Unix(2000, 0)}
	follower := newManager(store, "follower", followerClock)
	follower.reconcile(context.Background())
	if got, _ := follower.CurrentLease(); got.Data != "live.mp3" {
		t.Fatalf("follower data = %q", got.Data)
	}
	followerClock.Advance(DefaultTTL - time.Nanosecond)
	follower.reconcile(context.Background())
	if follower.IsLeader() {
		t.Fatal("follower took over before the index had been quiet for the TTL")
	}
	if _, ok := follower.CurrentLease(); !ok {
		t.Fatal("follower stopped exposing lease before the TTL")
	}
	followerClock.Advance(time.Nanosecond)
	if _, ok := follower.CurrentLease(); ok {
		t.Fatal("follower exposed a lease after the index had been quiet for the TTL")
	}
	follower.reconcile(context.Background())
	if !follower.IsLeader() || store.lease.Holder != "follower" {
		t.Fatal("follower did not take over after the index was quiet for the TTL")
	}
}

func TestReleaseLetsFollowerAcquireImmediately(t *testing.T) {
	store := &fakeStore{}
	clock := &fakeClock{now: time.Unix(1000, 0)}
	leader := newManager(store, "leader", clock)
	leader.reconcile(context.Background())
	follower := newManager(store, "follower", clock)
	follower.reconcile(context.Background())

	if err := leader.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	follower.reconcile(context.Background())
	if !follower.IsLeader() || store.lease.Holder != "follower" {
		t.Fatal("follower did not acquire the released lease immediately")
	}
}

func TestReleaseAndStaleIndexSafety(t *testing.T) {
	store := &fakeStore{}
	m := newManager(store, "one", nil)
	m.reconcile(context.Background())
	if err := m.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.IsLeader() || store.lease != nil {
		t.Fatal("release did not clear local and stored leadership")
	}

	replacementStore := &fakeStore{}
	replacement := newManager(replacementStore, "one", nil)
	replacement.reconcile(context.Background())
	replacementStore.mu.Lock()
	replacementStore.lease = &Lease[string]{Holder: "replacement"}
	replacementStore.index++
	replacementStore.mu.Unlock()
	if err := replacement.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if replacement.IsLeader() || replacementStore.lease.Holder != "replacement" {
		t.Fatal("release with a stale index removed a replacement lease")
	}
}

func TestTransientStoreErrorsPreserveLeadershipUntilExpiry(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	store := &fakeStore{}
	m := newManager(store, "one", clock)
	m.reconcile(context.Background())

	store.putErr = errors.New("temporary write failure")
	clock.Advance(DefaultRenewInterval)
	m.reconcile(context.Background())
	if !m.IsLeader() {
		t.Fatal("transient Put error dropped leadership before expiry")
	}

	store.putErr = nil
	store.getErr = errors.New("temporary read failure")
	clock.Advance(DefaultTTL - DefaultRenewInterval - time.Nanosecond)
	m.reconcile(context.Background())
	if !m.IsLeader() {
		t.Fatal("transient Get error dropped leadership before expiry")
	}

	clock.Advance(time.Nanosecond)
	m.reconcile(context.Background())
	if m.IsLeader() {
		t.Fatal("leadership remained active after local expiry")
	}
}

func TestOptionsDefaults(t *testing.T) {
	got := Options{TTL: time.Minute}.withDefaults()
	want := Options{TTL: time.Minute, RenewInterval: DefaultRenewInterval, PollInterval: DefaultPollInterval, OperationTimeout: DefaultOperationTimeout}
	if got != want {
		t.Fatalf("options = %+v, want %+v", got, want)
	}
}
