package cmd

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/longhorn/go-spdk-helper/pkg/initiator"
	"github.com/longhorn/longhorn-instance-manager/pkg/util"
	spdk "github.com/longhorn/longhorn-spdk-engine/pkg/spdk"
	"github.com/longhorn/types/pkg/generated/spdkrpc"
)

func NvmeAgentCmd() *cli.Command {
	return &cli.Command{
		Name:  "nvme-agent",
		Usage: "starts the NVMe CLI agent gRPC server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "listen",
				Value: initiator.GetDefaultNvmeCliAgentAddress(),
				Usage: "specifies the NVMe CLI agent endpoint to listen on",
			},
			&cli.StringFlag{
				Name:  "logs-dir",
				Value: "/var/log/instances",
			},
		},
		Action: func(_ context.Context, c *cli.Command) error {
			err := startNvmeAgent(c)
			if err != nil {
				logrus.WithError(err).Fatal("Failed to run nvme-agent command")
			}
			return err
		},
	}
}

func startNvmeAgent(c *cli.Command) (err error) {
	listen := c.String("listen")
	logsDir := c.String("logs-dir")

	if err := util.SetUpLogger(logsDir); err != nil {
		return err
	}

	var serverTLSConfig *tls.Config
	tlsDir := c.String("tls-dir")
	if tlsDir != "" {
		serverTLSConfig, _, err = loadTLSConfigsFromDir(tlsDir)
		if err != nil {
			logrus.WithError(err).Warnf("Failed to initialize TLS from %v; starting without TLS", tlsDir)
		}
	}

	grpcServer, listener, err := setupNvmeAgentGRPCServer(listen, serverTLSConfig)
	if err != nil {
		return err
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		logrus.Infof("NVMe CLI agent received %v to exit", sig)
		grpcServer.Stop()
	}()

	logrus.Infof("NVMe CLI agent listening to %v", listen)
	if err := grpcServer.Serve(listener); err != nil {
		logrus.WithError(err).Error("NVMe CLI agent failed to serve")
		return err
	}

	logrus.Info("Stopped NVMe CLI agent")
	return nil
}

func setupNvmeAgentGRPCServer(listen string, serverTLSConfig *tls.Config) (*grpc.Server, net.Listener, error) {
	if err := ensureUnixSocketParentDir(listen); err != nil {
		return nil, nil, err
	}

	srv, err := spdk.NewNvmeCliServer()
	if err != nil {
		return nil, nil, err
	}

	grpcServer, listener, err := util.NewServer(listen, serverTLSConfig,
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to setup NVMe CLI agent gRPC server")
	}

	spdkrpc.RegisterNvmeAgentServiceServer(grpcServer, srv)
	grpc_health_v1.RegisterHealthServer(grpcServer, &nvmeAgentHealthServer{})
	reflection.Register(grpcServer)

	return grpcServer, listener, nil
}

func ensureUnixSocketParentDir(endpoint string) error {
	const unixPrefix = "unix://"

	if !strings.HasPrefix(endpoint, unixPrefix) {
		return nil
	}

	socketPath := endpoint[len(unixPrefix):]
	if socketPath == "" {
		return errors.New("unix socket path is empty")
	}
	return os.MkdirAll(filepath.Dir(socketPath), 0755)
}

type nvmeAgentHealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
}

func (s *nvmeAgentHealthServer) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, nil
}

func (s *nvmeAgentHealthServer) List(context.Context, *grpc_health_v1.HealthListRequest) (*grpc_health_v1.HealthListResponse, error) {
	return &grpc_health_v1.HealthListResponse{
		Statuses: map[string]*grpc_health_v1.HealthCheckResponse{
			"": {
				Status: grpc_health_v1.HealthCheckResponse_SERVING,
			},
			spdkrpc.NvmeAgentService_ServiceDesc.ServiceName: {
				Status: grpc_health_v1.HealthCheckResponse_SERVING,
			},
		},
	}, nil
}

func (s *nvmeAgentHealthServer) Watch(_ *grpc_health_v1.HealthCheckRequest, ws grpc.ServerStreamingServer[grpc_health_v1.HealthCheckResponse]) error {
	return ws.Send(&grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	})
}
