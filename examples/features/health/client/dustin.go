package main

import (
	"fmt"
	"log/slog"
	"os"
	"sync"

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
	cc        balancer.ClientConn
	sc        balancer.SubConn
	pendingSC balancer.SubConn
	logger    *slog.Logger

	addresses []resolver.Address
	state     connectivity.State

	mu sync.Mutex
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
	db.mu.Lock()
	defer db.mu.Unlock()

	db.logger.Info("UpdateClientConnState called")

	db.addresses = state.ResolverState.Addresses

	sc, err := db.cc.NewSubConn(state.ResolverState.Addresses, balancer.NewSubConnOptions{
		HealthCheckEnabled: true,
		StateListener:      db.singleStateListener,
	})
	if err != nil {
		return fmt.Errorf("error creating sub connection: %w", err)
	}

	sc.Connect()

	db.sc = sc

	db.cc.UpdateState(balancer.State{
		ConnectivityState: connectivity.Connecting,
		Picker: dustinPicker{
			sc: db.sc,
		},
	})

	return nil
}

func (db *dustinBalancer) singleStateListener(state balancer.SubConnState) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.logger.Info("single StateListener invoked", slog.String("state", state.ConnectivityState.String()))

	if db.state == connectivity.Ready && state.ConnectivityState == connectivity.TransientFailure && db.pendingSC == nil {
		db.logger.Info("creating new subconnection")

		sc, err := db.cc.NewSubConn(db.addresses, balancer.NewSubConnOptions{
			HealthCheckEnabled: true,
			StateListener:      db.migrateStateListener,
		})
		if err != nil {
			db.logger.Error("error creating new subconnection", slog.String("err", err.Error()))
		}

		sc.Connect()

		db.pendingSC = sc
	} else if state.ConnectivityState == connectivity.Ready && db.pendingSC != nil {
		db.logger.Info("subconnection became healthy again, so shutting down new subconnection")

		db.pendingSC.RegisterHealthListener(func(balancer.SubConnState) {})
		db.pendingSC.Shutdown()
	}

	db.state = state.ConnectivityState
}

func (db *dustinBalancer) migrateStateListener(state balancer.SubConnState) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.logger.Info("migrate StateListener invoked", slog.String("state", state.ConnectivityState.String()))

	if state.ConnectivityState == connectivity.Ready && db.pendingSC != nil {
		db.logger.Info("new subconnection is ready, so migrating")

		db.sc.RegisterHealthListener(func(balancer.SubConnState) {})

		db.pendingSC.RegisterHealthListener(db.singleStateListener)

		db.cc.UpdateState(balancer.State{
			ConnectivityState: connectivity.Ready,
			Picker: dustinPicker{
				sc: db.pendingSC,
			},
		})

		db.sc.Shutdown()

		db.sc = db.pendingSC

		db.pendingSC = nil

		db.state = connectivity.Ready
	} else if state.ConnectivityState == connectivity.TransientFailure {
		if db.pendingSC != nil {
			db.logger.Info("new subconnection hit failure, so shutting down")

			db.logger.Info("seconday sleeping")
			time.Sleep(5 * time.Second)

			db.pendingSC.RegisterHealthListener(func(balancer.SubConnState) {})
			db.pendingSC.Shutdown()

			sc, err := db.cc.NewSubConn(db.addresses, balancer.NewSubConnOptions{
				HealthCheckEnabled: true,
				StateListener:      db.migrateStateListener,
			})
			if err != nil {
				db.logger.Error("error creating new subconnection", slog.String("err", err.Error()))
			}

			sc.Connect()

			db.pendingSC = sc

			db.cc.UpdateState(balancer.State{
				ConnectivityState: connectivity.Ready,
				Picker: dustinPicker{
					sc: db.pendingSC,
				},
			})

			return
		}

		db.logger.Info("primary connection hit failure, shutting down")

		db.sc.RegisterHealthListener(func(balancer.SubConnState) {})
		db.sc.Shutdown()

		sc, err := db.cc.NewSubConn(db.addresses, balancer.NewSubConnOptions{
			HealthCheckEnabled: true,
			StateListener:      db.singleStateListener,
		})
		if err != nil {
			db.logger.Error("error creating new primary subconnection", slog.String("err", err.Error()))
		}

		sc.Connect()

		db.sc = sc

		db.cc.UpdateState(balancer.State{
			ConnectivityState: connectivity.Ready,
			Picker: dustinPicker{
				sc: db.sc,
			},
		})
	} else if state.ConnectivityState == connectivity.Shutdown {
		db.logger.Info("new subconnection has completed shutdown")
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
