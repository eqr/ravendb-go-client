package ravenlease

import (
	"context"
	"sync"
	"time"
)

const (
	DefaultTTL              = 15 * time.Second
	DefaultRenewInterval    = 5 * time.Second
	DefaultPollInterval     = time.Second
	DefaultOperationTimeout = 2 * time.Second
)

// Lease is the value stored for the current leader. Data carries application state that followers can read.
type Lease[T any] struct {
	Holder string `json:"holder"`
	Data   T      `json:"data"`
}

// Store reads and writes a lease with optimistic concurrency on index.
// Put with index 0 creates the lease only when it is missing.
type Store[T any] interface {
	Get(ctx context.Context) (*Lease[T], int64, error)
	Put(ctx context.Context, lease Lease[T], index int64) (int64, bool, error)
	Delete(ctx context.Context, index int64) (bool, error)
}

// Options tunes lease timing. Zero fields use the Default values.
type Options struct {
	TTL              time.Duration
	RenewInterval    time.Duration
	PollInterval     time.Duration
	OperationTimeout time.Duration
}

func (o Options) withDefaults() Options {
	if o.TTL <= 0 {
		o.TTL = DefaultTTL
	}
	if o.RenewInterval <= 0 {
		o.RenewInterval = DefaultRenewInterval
	}
	if o.PollInterval <= 0 {
		o.PollInterval = DefaultPollInterval
	}
	if o.OperationTimeout <= 0 {
		o.OperationTimeout = DefaultOperationTimeout
	}
	return o
}

// Manager elects one leader among processes sharing a Store.
// Leadership is timed by the local clock only: a leader trusts its lease for TTL after the write was sent,
// and a follower takes over once the stored index has not changed for TTL.
type Manager[T any] struct {
	store  Store[T]
	holder string
	opts   Options
	now    func() time.Time

	opMu        sync.Mutex
	mu          sync.RWMutex
	lease       Lease[T]
	index       int64
	seenAt      time.Time
	hasSeen     bool
	leader      bool
	localExpiry time.Time
	data        T
	dataSet     bool
	changes     chan struct{}
	wake        chan struct{}
}

// NewManager returns a manager that competes for the lease as holder. holder must be unique per process.
func NewManager[T any](store Store[T], holder string, opts Options) *Manager[T] {
	return &Manager[T]{
		store:   store,
		holder:  holder,
		opts:    opts.withDefaults(),
		now:     time.Now,
		changes: make(chan struct{}, 1),
		wake:    make(chan struct{}, 1),
	}
}

// Run keeps the lease reconciled until ctx is done. It does not release the lease; call Release after Run returns.
func (m *Manager[T]) Run(ctx context.Context) {
	ticker := time.NewTicker(m.opts.PollInterval)
	defer ticker.Stop()
	m.reconcile(ctx)
	lastRenew := m.now()
	for {
		m.mu.RLock()
		expiryAt, leader := m.localExpiry, m.leader
		m.mu.RUnlock()
		var expiry *time.Timer
		var expiryC <-chan time.Time
		if leader {
			expiry = time.NewTimer(max(expiryAt.Sub(m.now()), 0))
			expiryC = expiry.C
		}
		select {
		case <-ctx.Done():
			if expiry != nil {
				expiry.Stop()
			}
			m.setNotLeader()
			return
		case <-ticker.C:
			if expiry != nil {
				expiry.Stop()
			}
			if m.IsLeader() && m.now().Sub(lastRenew) < m.opts.RenewInterval {
				m.refresh(ctx)
			} else {
				m.reconcile(ctx)
				lastRenew = m.now()
			}
		case <-m.wake:
			if expiry != nil {
				expiry.Stop()
			}
			m.reconcile(ctx)
			lastRenew = m.now()
		case <-expiryC:
			m.setNotLeader()
			m.reconcile(ctx)
			lastRenew = m.now()
		}
	}
}

// IsLeader reports whether this process holds an unexpired lease.
func (m *Manager[T]) IsLeader() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.leader && m.now().Before(m.localExpiry)
}

// Changes receives a signal when IsLeader may have changed. Signals coalesce.
func (m *Manager[T]) Changes() <-chan struct{} { return m.changes }

// SetData sets the data this process writes when it holds the lease, and triggers a write.
func (m *Manager[T]) SetData(data T) {
	m.mu.Lock()
	m.data = data
	m.dataSet = true
	if m.leader && m.now().Before(m.localExpiry) {
		m.lease.Data = data
	}
	m.mu.Unlock()
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// CurrentLease returns the last lease seen while it is still considered alive.
func (m *Manager[T]) CurrentLease() (Lease[T], bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.leaseAliveLocked(m.now()) || m.lease.Holder == "" {
		return Lease[T]{}, false
	}
	return m.lease, true
}

// Release gives up leadership and deletes the lease if this process still owns it.
func (m *Manager[T]) Release(ctx context.Context) error {
	m.opMu.Lock()
	defer m.opMu.Unlock()
	m.mu.RLock()
	lease, index := m.lease, m.index
	m.mu.RUnlock()
	m.setNotLeader()
	if lease.Holder != m.holder || index <= 0 {
		return nil
	}
	_, err := m.delete(ctx, index)
	return err
}

func (m *Manager[T]) leaseAliveLocked(now time.Time) bool {
	if m.leader {
		return now.Before(m.localExpiry)
	}
	return m.hasSeen && now.Sub(m.seenAt) < m.opts.TTL
}

func (m *Manager[T]) reconcile(ctx context.Context) {
	m.opMu.Lock()
	defer m.opMu.Unlock()
	lease, index, err := m.get(ctx)
	if err != nil {
		return
	}
	m.reconcileLease(ctx, lease, index, true)
}

func (m *Manager[T]) refresh(ctx context.Context) {
	m.opMu.Lock()
	defer m.opMu.Unlock()
	lease, index, err := m.get(ctx)
	if err != nil {
		return
	}
	m.reconcileLease(ctx, lease, index, false)
}

func (m *Manager[T]) reconcileLease(ctx context.Context, lease *Lease[T], index int64, forceRenew bool) {
	if lease != nil {
		seenAt, wasLeader := m.observeLease(*lease, index, m.now())
		if m.now().Sub(seenAt) < m.opts.TTL && (lease.Holder != m.holder || !wasLeader || !forceRenew) {
			return
		}
	} else {
		m.observeMissing(index, m.now())
	}

	m.mu.RLock()
	data, dataSet := m.data, m.dataSet
	m.mu.RUnlock()
	if lease != nil && lease.Holder == m.holder && !dataSet {
		data = lease.Data
	}
	next := Lease[T]{Holder: m.holder, Data: data}
	sentAt := m.now()
	newIndex, success, err := m.put(ctx, next, index)
	if err != nil {
		return
	}
	if !success {
		m.dropLeadership(ctx, index, lease != nil && lease.Holder == m.holder)
		return
	}
	m.setLeaderSnapshot(next, newIndex, sentAt)
}

func (m *Manager[T]) observeLease(lease Lease[T], index int64, now time.Time) (time.Time, bool) {
	m.mu.Lock()
	wasLeader := m.leader && now.Before(m.localExpiry)
	wasLeaderState := m.leader
	if !m.hasSeen || m.index != index {
		m.seenAt = now
		m.hasSeen = true
	}
	if lease.Holder == m.holder && wasLeader && m.dataSet {
		lease.Data = m.data
	}
	m.lease, m.index = lease, index
	m.leader = wasLeader && lease.Holder == m.holder
	changed := wasLeaderState != m.leader
	seenAt := m.seenAt
	m.mu.Unlock()
	if changed {
		m.notify()
	}
	return seenAt, wasLeader
}

func (m *Manager[T]) observeMissing(index int64, now time.Time) {
	m.mu.Lock()
	wasLeaderState := m.leader
	if !m.hasSeen || m.index != index {
		m.seenAt = now
		m.hasSeen = true
	}
	m.lease, m.index = Lease[T]{}, index
	m.leader = m.leader && now.Before(m.localExpiry)
	changed := wasLeaderState != m.leader
	m.mu.Unlock()
	if changed {
		m.notify()
	}
}

func (m *Manager[T]) setLeaderSnapshot(lease Lease[T], index int64, sentAt time.Time) {
	m.mu.Lock()
	wasLeader := m.leader
	if !m.hasSeen || m.index != index {
		m.seenAt = sentAt
		m.hasSeen = true
	}
	if m.dataSet {
		lease.Data = m.data
	}
	m.lease, m.index = lease, index
	m.localExpiry = sentAt.Add(m.opts.TTL)
	m.leader = m.now().Before(m.localExpiry)
	changed := wasLeader != m.leader
	m.mu.Unlock()
	if changed {
		m.notify()
	}
}

// A failed renew of our own lease means someone else wrote in between; delete what we may have left behind.
func (m *Manager[T]) dropLeadership(ctx context.Context, index int64, observedOwn bool) {
	m.mu.RLock()
	wasLeader, previousLease, previousIndex := m.leader, m.lease, m.index
	m.mu.RUnlock()
	m.setNotLeader()
	if !observedOwn {
		if index != 0 || !wasLeader || previousLease.Holder != m.holder {
			return
		}
		index = previousIndex
	}
	if index > 0 {
		_, _ = m.delete(ctx, index)
	}
}

func (m *Manager[T]) setNotLeader() {
	m.mu.Lock()
	changed := m.leader
	m.leader = false
	m.mu.Unlock()
	if changed {
		m.notify()
	}
}

func (m *Manager[T]) notify() {
	select {
	case m.changes <- struct{}{}:
	default:
	}
}

func (m *Manager[T]) get(ctx context.Context) (*Lease[T], int64, error) {
	ctx, cancel := context.WithTimeout(ctx, m.opts.OperationTimeout)
	defer cancel()
	return m.store.Get(ctx)
}

func (m *Manager[T]) put(ctx context.Context, lease Lease[T], index int64) (int64, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, m.opts.OperationTimeout)
	defer cancel()
	return m.store.Put(ctx, lease, index)
}

func (m *Manager[T]) delete(ctx context.Context, index int64) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, m.opts.OperationTimeout)
	defer cancel()
	return m.store.Delete(ctx, index)
}
