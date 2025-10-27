package main

import (
	"fmt"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/balancer/pickfirst/pickfirstleaf"
	"google.golang.org/grpc/connectivity"
)

func init() {
	balancer.Register(teleportLBBuilder{})
}

type teleportLBBuilder struct{}

func (teleportLBBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	pflb := balancer.Get(pickfirstleaf.Name)

	wcc := wrappedCC{
		ClientConn: cc,
		opts:       opts,
	}

	b := pflb.Build(&wcc, opts)

	primary := &wrappedBalancer{
		Balancer: b,
	}

	wcc.primary = primary

	return primary
}

func (teleportLBBuilder) Name() string {
	return "teleport_lb"
}

type wrappedBalancer struct {
	balancer.Balancer

	ccs balancer.ClientConnState
}

func (w *wrappedBalancer) UpdateClientConnState(ccs balancer.ClientConnState) error {
	fmt.Println("update client conn state called")

	fmt.Printf("resolved addresses: %v\n", ccs.ResolverState.Addresses)

	// enable pickfirstleaf's health listener so transient failure state updates may
	// be handled
	ccs.ResolverState = pickfirstleaf.EnableHealthListener(ccs.ResolverState)

	w.ccs = ccs

	return w.Balancer.UpdateClientConnState(ccs)
}

type wrappedCC struct {
	balancer.ClientConn
	opts balancer.BuildOptions

	primary *wrappedBalancer

	pending balancer.Balancer
}

func (w *wrappedCC) UpdateState(state balancer.State) {
	fmt.Printf("update state called with %s\n", state.ConnectivityState)
	if state.ConnectivityState == connectivity.TransientFailure {
		if w.pending == nil {
			fmt.Println("creating new balancer")

			pflb := balancer.Get(pickfirstleaf.Name)

			b := pflb.Build(w, w.opts)

			w.pending = b

			b.UpdateClientConnState(w.primary.ccs)

			w.ClientConn.UpdateState(balancer.State{
				ConnectivityState: connectivity.Connecting,
				Picker:            base.NewErrPicker(balancer.ErrNoSubConnAvailable),
			})
		} else {
			fmt.Println("new balancer already created")
		}
	} else {
		w.ClientConn.UpdateState(state)
	}
}

type tlbPicker struct {
	sc balancer.SubConn
}

func (tlbPicker tlbPicker) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	return balancer.PickResult{
		SubConn: tlbPicker.sc,
	}, nil
}
