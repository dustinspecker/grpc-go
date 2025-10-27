package main

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/balancer/pickfirst/pickfirstleaf"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

func init() {
	balancer.Register(tlbBuilder{})
}

type tlbBuilder struct{}

func (tlbBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	fmt.Println("tlbBuilder.Build called")

	b := tlbBalancer{
		cc: &wCC{
			ClientConn: cc,
		},
		opts: opts,
	}

	wb := tlbWrappedBalancer{
		ClientConn: cc,

		tlb:      &b,
		subConns: make(map[balancer.SubConn]bool, 0),
	}

	pflb := balancer.Get(pickfirstleaf.Name).Build(&wb, opts)

	wb.Balancer = pflb

	b.current = &wb

	return &b
}

type wCC struct {
	balancer.ClientConn
}

func (w *wCC) NewSubConn(addrs []resolver.Address, opts balancer.NewSubConnOptions) (balancer.SubConn, error) {
	fmt.Println("wcc.NewSubConn called")

	return w.ClientConn.NewSubConn(addrs, opts)
}

func (w *wCC) UpdateState(state balancer.State) {
	fmt.Println("w.UpdateState called")

	w.ClientConn.UpdateState(state)
}

func (tlbBuilder) Name() string {
	fmt.Println("tlbBuilder.Name called")

	return "tlb"
}

type tlbBalancer struct {
	cc   balancer.ClientConn
	opts balancer.BuildOptions

	current *tlbWrappedBalancer
	pending *tlbWrappedBalancer

	resolvedState resolver.State

	mu sync.Mutex
}

func (t *tlbBalancer) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	fmt.Println("tlbBalancer.Close called")

	if t.pending != nil {
		t.pending.Close()
	}

	if t.current != nil {
		t.current.Close()
	}

	t.pending = nil
	t.current = nil
}

func (t *tlbBalancer) ExitIdle() {
	t.mu.Lock()
	defer t.mu.Unlock()

	fmt.Println("tlbBalancer.ExitIdle called")
	bal := t.newestBalancer()

	if bal == nil {
		return
	}

	bal.ExitIdle()
}

func (t *tlbBalancer) ResolverError(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	fmt.Println("tlbBalancer.ResolverError called")
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

func (t *tlbBalancer) UpdateClientConnState(state balancer.ClientConnState) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	fmt.Println("tlbBalancer.UpdateClientConnState called")
	bal := t.newestBalancer()

	if bal == nil {
		return errors.New("balancer closed")
	}

	t.resolvedState = state.ResolverState

	state.ResolverState = pickfirstleaf.EnableHealthListener(state.ResolverState)

	return bal.UpdateClientConnState(state)
}

func (t *tlbBalancer) UpdateSubConnState(sc balancer.SubConn, scs balancer.SubConnState) {
	t.mu.Lock()
	defer t.mu.Unlock()

	fmt.Printf("tlbBalancer.UpdateSubConnState called with %s\n", scs.ConnectivityState)

	var bal *tlbWrappedBalancer

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

func (t *tlbBalancer) newestBalancer() balancer.Balancer {
	if t.pending != nil {
		return t.pending
	}

	return t.current
}

type tlbWrappedBalancer struct {
	balancer.ClientConn
	balancer.Balancer

	tlb *tlbBalancer

	subConns map[balancer.SubConn]bool
}

func (t *tlbWrappedBalancer) Close() {
	fmt.Println("tlbWrappedBalancer.Close called")
	if t == nil {
		return
	}

	for sc := range t.subConns {
		sc.Shutdown()
	}
}

func (t *tlbWrappedBalancer) NewSubConn(addrs []resolver.Address, opts balancer.NewSubConnOptions) (balancer.SubConn, error) {
	fmt.Println("tlbWrappedBalancer.NewSubConn called")
	if t != t.tlb.current && t != t.tlb.pending {
		return nil, errors.New("balancer that called NewSubConn is closed")
	}

	origListener := opts.StateListener

	var sc balancer.SubConn

	opts.StateListener = func(state balancer.SubConnState) {
		fmt.Printf("state listener called with %s\n", state.ConnectivityState)

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

func (t *tlbWrappedBalancer) ResolveNow(opts resolver.ResolveNowOptions) {
	fmt.Println("tlbWrappedBalancer.ResolveNow called")
	if t != t.tlb.newestBalancer() {
		return
	}

	t.tlb.cc.ResolveNow(opts)
}

func (t *tlbWrappedBalancer) RemoveSubConn(sc balancer.SubConn) {
	fmt.Println("tlbWrappedBalancer.RemoveSubConn called")
	sc.Shutdown()
}

func (t *tlbWrappedBalancer) UpdateAddresses(sc balancer.SubConn, addrs []resolver.Address) {
	fmt.Println("tlbWrappedBalancer.UpdateAddresses called")
	if t != t.tlb.current && t != t.tlb.pending {
		return
	}

	t.tlb.cc.UpdateAddresses(sc, addrs)
}

func (t *tlbWrappedBalancer) UpdateSubConnState(sc balancer.SubConn, scs balancer.SubConnState) {
	fmt.Println("tlbWrappedBalancer.UpdateSubConnState called")

	t.Balancer.UpdateSubConnState(sc, scs)
}

func (t *tlbWrappedBalancer) UpdateState(state balancer.State) {
	fmt.Printf("tlbWrappedBalancer.UpdateState called with %s\n", state.ConnectivityState)

	if t != t.tlb.current && t != t.tlb.pending {
		return
	}

	if t == t.tlb.current {
		if state.ConnectivityState == connectivity.TransientFailure {
			fmt.Println("creating new balancer")
			wb := tlbWrappedBalancer{
				ClientConn: t.tlb.cc,

				tlb:      t.tlb,
				subConns: make(map[balancer.SubConn]bool, 0),
			}

			pflb := balancer.Get(pickfirstleaf.Name).Build(&wb, t.tlb.opts)

			wb.Balancer = pflb

			t.tlb.pending = &wb

			pflb.UpdateClientConnState(balancer.ClientConnState{
				ResolverState: pickfirstleaf.EnableHealthListener(t.tlb.resolvedState),
			})
		} else if state.ConnectivityState == connectivity.Ready && t.tlb.pending != nil {
			fmt.Println("original balancer became healthy, closing pending balancer")

			pending := t.tlb.pending

			t.tlb.pending = nil

			pending.Close()
		}
	} else if t == t.tlb.pending {
		fmt.Println("update state called for pending")
		if state.ConnectivityState == connectivity.Ready {
			fmt.Println("migrating to new balancer")

			current := t.tlb.current

			t.tlb.current = t.tlb.pending
			t.tlb.pending = nil

			current.Close()
		} else if state.ConnectivityState == connectivity.TransientFailure {
			fmt.Println("new balancer is unhealthy, recreating new balancer")

			t.tlb.pending.Close()

			time.Sleep(1 * time.Second)

			wb := tlbWrappedBalancer{
				ClientConn: t.tlb.cc,

				tlb:      t.tlb,
				subConns: make(map[balancer.SubConn]bool, 0),
			}

			pflb := balancer.Get(pickfirstleaf.Name).Build(&wb, t.tlb.opts)

			wb.Balancer = pflb

			t.tlb.pending = &wb

			pflb.UpdateClientConnState(balancer.ClientConnState{
				ResolverState: pickfirstleaf.EnableHealthListener(t.tlb.resolvedState),
			})
		}
	} else {
		fmt.Println("UpdateState called for invalid balancer")
	}

	t.tlb.cc.UpdateState(state)
}
