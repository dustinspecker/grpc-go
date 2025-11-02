/*
 *
 * Copyright 2020 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Binary client demonstrates how to check and observe gRPC server health using
// the health library.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/examples/features/health/client/client"
	pb "google.golang.org/grpc/examples/features/proto/echo"
	_ "google.golang.org/grpc/health"
)

var serviceConfig = fmt.Sprintf(`{
	"loadBalancingConfig": [{"%s":{}}],
	"healthCheckConfig": {
		"serviceName": ""
	}
}`, client.Name)

func callUnaryEcho(ctx context.Context, c pb.EchoClient) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	r, err := c.UnaryEcho(ctx, &pb.EchoRequest{})
	if err != nil {
		fmt.Println("UnaryEcho: _, ", err)
	} else {
		fmt.Println("UnaryEcho: ", r.GetMessage())
	}
}

func main() {
	flag.Parse()

	// r := manual.NewBuilderWithScheme("whatever")
	// r.InitialState(resolver.State{
	// 	Addresses: []resolver.Address{
	// 		{Addr: "127.0.0.1:50050"},
	// 		{Addr: "127.0.0.2:50050"},
	// 	},
	// })

	address := "localhost:50050"
	// address := fmt.Sprintf("%s:///unused", r.Scheme())

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		// grpc.WithResolvers(r),
		grpc.WithDefaultServiceConfig(serviceConfig),
	}

	conn, err := grpc.NewClient(address, options...)
	if err != nil {
		log.Fatalf("grpc.NewClient(%q): %v", address, err)
	}
	defer conn.Close()

	echoClient := pb.NewEchoClient(conn)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	stream, err := echoClient.BidirectionalStreamingEcho(ctx)
	if err != nil {
		fmt.Println(fmt.Sprintf("error creating streamer: %v", err))

		return
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		done := false
		for !done {
			select {
			case <-ctx.Done():
				done = true
			default:
				callUnaryEcho(ctx, echoClient)
				time.Sleep(time.Second)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				resp, err := stream.Recv()
				if err != nil {
					fmt.Println(fmt.Sprintf("error receiving message: %v", err))

					cancel()

					return
				}

				fmt.Println(resp.Message)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		done := false
		i := 0
		for !done {
			select {
			case <-ctx.Done():
				done = true
			default:
				req := pb.EchoRequest{
					Message: fmt.Sprintf("hey %d", i),
				}
				if err := stream.Send(&req); err != nil {
					fmt.Println(fmt.Sprintf("error sending request: %v", err))

					cancel()
				}

				time.Sleep(1 * time.Second)
				i++
			}
		}

		stream.CloseSend()
	}()

	wg.Wait()
}
