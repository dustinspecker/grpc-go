package main

import (
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/balancer/pickfirst/pickfirstleaf"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

const (
	Name = "teleport_pick_healthy"
)

func init() {
	balancer.Register(teleportPickHealthyBuilder{})
}

type teleportPickHealthyBuilder struct{}

func (teleportPickHealthyBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	b := teleportPickHealthyBalancer{
		cc:   cc,
		opts: opts,
		log:  log,
	}

	wb := wrappedBalancer{
		ClientConn: cc,
		log:        log,

		tlb:      &b,
		subConns: make(map[balancer.SubConn]bool, 0),
	}

	pflb := balancer.Get(pickfirstleaf.Name).Build(&wb, opts)

	wb.Balancer = pflb

	b.current = &wb

	return &b
}

func (teleportPickHealthyBuilder) Name() string {
	return Name
}

type teleportPickHealthyBalancer struct {
	cc   balancer.ClientConn
	opts balancer.BuildOptions
	log  *slog.Logger

	current *wrappedBalancer
	pending *wrappedBalancer

	resolvedState resolver.State

	mu sync.Mutex
}

func (t *teleportPickHealthyBalancer) Close() {
	t.log.Info("tlbBalancer.Close called")

	t.mu.Lock()

	current := t.current
	pending := t.pending

	t.current = nil
	t.pending = nil

	t.mu.Unlock()

	if current != nil {
		current.Close()
	}

	if pending != nil {
		pending.Close()
	}
}

func (t *teleportPickHealthyBalancer) ExitIdle() {
	t.log.Info("tlbBalancer.ExitIdle called")
	bal := t.newestBalancer()

	if bal == nil {
		return
	}

	bal.ExitIdle()
}

func (t *teleportPickHealthyBalancer) ResolverError(err error) {
	t.log.Info("tlbBalancer.ResolverError called")
	bal := t.newestBalancer()

	if bal == nil {
		t.cc.UpdateState(balancer.State{
			ConnectivityState: connectivity.TransientFailure,
			Picker:            base.NewErrPicker(err),
		})

		return
	}

	bal.ResolverError(err)
}

func (t *teleportPickHealthyBalancer) UpdateClientConnState(state balancer.ClientConnState) error {
	t.log.Info("tlbBalancer.UpdateClientConnState called")
	bal := t.newestBalancer()

	if bal == nil {
		return errors.New("balancer closed")
	}

	t.resolvedState = state.ResolverState

	state.ResolverState = pickfirstleaf.EnableHealthListener(state.ResolverState)

	return bal.UpdateClientConnState(state)
}

func (t *teleportPickHealthyBalancer) UpdateSubConnState(sc balancer.SubConn, scs balancer.SubConnState) {
	t.log.Info("tlbBalancer.UpdateSubConnState called", slog.String("state", scs.ConnectivityState.String()))

	t.mu.Lock()
	defer t.mu.Unlock()

	var bal *wrappedBalancer

	if t.current != nil && t.current.subConns[sc] {
		bal = t.current
	} else if t.pending != nil && t.pending.subConns[sc] {
		bal = t.pending
	}

	if bal == nil {
		return
	}

	if scs.ConnectivityState == connectivity.Shutdown {
		delete(bal.subConns, sc)
	}

	// Do not invoke bal.UpdateSubConnState since the pick_first_leaf does not exist UpdateSubConnState to be invoked,
	// since it uses state listeners instead.
	// bal.UpdateSubConnState(sc, scs)
}

func (t *teleportPickHealthyBalancer) newestBalancer() balancer.Balancer {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.pending != nil {
		return t.pending
	}

	return t.current
}

type wrappedBalancer struct {
	balancer.ClientConn
	balancer.Balancer
	log *slog.Logger

	tlb *teleportPickHealthyBalancer

	subConns map[balancer.SubConn]bool
}

func (t *wrappedBalancer) Close() {
	t.log.Info("tlbWrappedBalancer.Close called")
	if t == nil {
		return
	}

	for sc := range t.subConns {
		sc.Shutdown()
	}
}

func (t *wrappedBalancer) NewSubConn(addrs []resolver.Address, opts balancer.NewSubConnOptions) (balancer.SubConn, error) {
	t.log.Info("tlbWrappedBalancer.NewSubConn called")
	t.tlb.mu.Lock()

	if t != t.tlb.current && t != t.tlb.pending {
		t.tlb.mu.Unlock()
		return nil, errors.New("balancer that called NewSubConn is closed")
	}

	t.tlb.mu.Unlock()

	origListener := opts.StateListener

	var sc balancer.SubConn

	opts.StateListener = func(state balancer.SubConnState) {
		t.log.Info("state listener called", slog.String("state", state.ConnectivityState.String()))

		t.tlb.UpdateSubConnState(sc, state)

		if origListener != nil {
			origListener(state)
		}
	}

	sc, err := t.tlb.cc.NewSubConn(addrs, opts)
	if err != nil {
		return nil, err
	}

	t.subConns[sc] = true

	return sc, nil
}

func (t *wrappedBalancer) ResolveNow(opts resolver.ResolveNowOptions) {
	t.log.Info("tlbWrappedBalancer.ResolveNow called")
	if t != t.tlb.newestBalancer() {
		return
	}

	t.tlb.cc.ResolveNow(opts)
}

func (t *wrappedBalancer) RemoveSubConn(sc balancer.SubConn) {
	t.log.Info("tlbWrappedBalancer.RemoveSubConn called")
	sc.Shutdown()
}

func (t *wrappedBalancer) UpdateAddresses(sc balancer.SubConn, addrs []resolver.Address) {
	t.log.Info("tlbWrappedBalancer.UpdateAddresses called")
	t.tlb.mu.Lock()
	if t != t.tlb.current && t != t.tlb.pending {
		t.tlb.mu.Unlock()
		return
	}

	t.tlb.mu.Unlock()

	t.tlb.cc.UpdateAddresses(sc, addrs)
}

func (t *wrappedBalancer) UpdateSubConnState(sc balancer.SubConn, scs balancer.SubConnState) {
	t.log.Info("tlbWrappedBalancer.UpdateSubConnState called")

	t.Balancer.UpdateSubConnState(sc, scs)
}

func (t *wrappedBalancer) UpdateState(state balancer.State) {
	t.log.Info("tlbWrappedBalancer.UpdateState called", slog.String("state", state.ConnectivityState.String()))
	t.tlb.mu.Lock()

	if t != t.tlb.current && t != t.tlb.pending {
		t.tlb.mu.Unlock()
		return
	}

	if t == t.tlb.current {
		if state.ConnectivityState == connectivity.TransientFailure {
			t.log.Info("creating new balancer")
			wb := wrappedBalancer{
				ClientConn: t.tlb.cc,
				log:        t.log,

				tlb:      t.tlb,
				subConns: make(map[balancer.SubConn]bool, 0),
			}

			pflb := balancer.Get(pickfirstleaf.Name).Build(&wb, t.tlb.opts)

			wb.Balancer = pflb

			t.tlb.pending = &wb

			t.tlb.mu.Unlock()

			pflb.UpdateClientConnState(balancer.ClientConnState{
				ResolverState: pickfirstleaf.EnableHealthListener(t.tlb.resolvedState),
			})
		} else if state.ConnectivityState == connectivity.Ready && t.tlb.pending != nil {
			t.log.Info("original balancer became healthy, closing pending balancer")

			pending := t.tlb.pending

			t.tlb.pending = nil

			t.tlb.mu.Unlock()

			pending.Close()
		} else {
			t.tlb.mu.Unlock()
		}
	} else if t == t.tlb.pending {
		t.log.Info("update state called for pending")
		if state.ConnectivityState == connectivity.Ready {
			t.log.Info("migrating to new balancer")

			current := t.tlb.current

			t.tlb.current = t.tlb.pending
			t.tlb.pending = nil

			t.tlb.mu.Unlock()

			current.Close()
		} else if state.ConnectivityState == connectivity.TransientFailure {
			t.log.Info("new balancer is unhealthy, recreating new balancer")

			t.tlb.pending.Close()

			t.tlb.mu.Unlock()

			time.Sleep(1 * time.Second)

			t.tlb.mu.Lock()

			wb := wrappedBalancer{
				ClientConn: t.tlb.cc,
				log:        t.log,

				tlb:      t.tlb,
				subConns: make(map[balancer.SubConn]bool, 0),
			}

			pflb := balancer.Get(pickfirstleaf.Name).Build(&wb, t.tlb.opts)

			wb.Balancer = pflb

			t.tlb.pending = &wb

			t.tlb.mu.Unlock()

			pflb.UpdateClientConnState(balancer.ClientConnState{
				ResolverState: pickfirstleaf.EnableHealthListener(t.tlb.resolvedState),
			})
		} else {
			t.tlb.mu.Unlock()
		}
	} else {
		t.log.Info("UpdateState called for invalid balancer")
		t.tlb.mu.Unlock()
	}

	t.tlb.cc.UpdateState(state)
}
