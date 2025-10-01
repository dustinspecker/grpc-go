package main

import (
	"fmt"
	"log/slog"
	"os"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

func init() {
	balancer.Register(dustinBuilder{})
}

type dustinBuilder struct{}

func (dustinBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	b := &dustinBalancer{
		cc:     cc,
		logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}

	return b
}

func (dustinBuilder) Name() string {
	return "dustin"
}

type dustinBalancer struct {
	cc     balancer.ClientConn
	sc     balancer.SubConn
	logger *slog.Logger

	addresses []resolver.Address

	state connectivity.State
}

func (db *dustinBalancer) Close() {
	db.logger.Info("Close called")

	db.sc.Shutdown()
}

func (db *dustinBalancer) ExitIdle() {
	db.logger.Info("ExitIdle called")
}

func (db *dustinBalancer) ResolverError(err error) {
	db.logger.Info("ResolverError called")
}

func (db *dustinBalancer) UpdateClientConnState(state balancer.ClientConnState) error {
	db.logger.Info("UpdateClientConnState called")

	db.addresses = state.ResolverState.Addresses

	sc, err := db.cc.NewSubConn(state.ResolverState.Addresses, balancer.NewSubConnOptions{
		HealthCheckEnabled: true,
		StateListener:      db.stateListener,
	})
	if err != nil {
		return fmt.Errorf("error creating sub connection: %w", err)
	}
	sc.Connect()
	db.sc = sc

	// TODO: have state on the balancer itself
	db.cc.UpdateState(balancer.State{
		ConnectivityState: connectivity.Connecting,
		Picker: dustinPicker{
			sc: db.sc,
		},
	})

	return nil
}

func (db *dustinBalancer) stateListener(state balancer.SubConnState) {
	db.logger.Info("StateListener invoked", slog.String("state", state.ConnectivityState.String()))

	if db.state == connectivity.Ready && state.ConnectivityState == connectivity.TransientFailure {
		db.logger.Info("creating new subconnection")

		// TODO: get new addresses?
		sc, err := db.cc.NewSubConn(db.addresses, balancer.NewSubConnOptions{
			HealthCheckEnabled: true,
			StateListener:      db.stateListener,
		})
		if err != nil {
			db.logger.Error("error creating new subconnection after transient failure: %v", err)

			return
		}

		sc.Connect()

		oldSc := db.sc
		oldSc.Shutdown()

		db.sc = sc

		db.state = connectivity.Connecting

		db.cc.UpdateState(balancer.State{
			ConnectivityState: connectivity.Connecting,
			Picker: dustinPicker{
				sc: db.sc,
			},
		})
	} else {
		db.state = state.ConnectivityState
	}
}

func (db *dustinBalancer) UpdateSubConnState(sc balancer.SubConn, scs balancer.SubConnState) {
	db.logger.Info("UpdateSubConnState called")
}

type dustinPicker struct {
	sc balancer.SubConn
}

func (dp dustinPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	return balancer.PickResult{
		SubConn: dp.sc,
	}, nil
}
