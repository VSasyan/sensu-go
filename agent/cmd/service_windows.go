package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	runtimedebug "runtime/debug"
	"sync"
	"syscall"

	"github.com/sensu/sensu-go/agent"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/debug"
	"golang.org/x/sys/windows/svc/eventlog"
)

var (
	elog         debug.Log
	AgentNewFunc = agent.NewAgentContext
)

func NewService(cfg *agent.Config) *Service {
	return &Service{cfg: cfg}
}

type Service struct {
	cfg *agent.Config
	wg  sync.WaitGroup
	mu  sync.Mutex
}

func (s *Service) start(ctx context.Context, cancel context.CancelFunc, changes chan<- svc.Status) chan error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wg.Wait()
	s.wg.Add(1)
	result := make(chan error, 1)
	go func() {
		defer func() {
			if e := recover(); e != nil {
				changes <- svc.Status{State: svc.Stopped}
				stack := runtimedebug.Stack()
				result <- errors.New(string(stack))
			}
		}()
		defer s.wg.Done()
		changes <- svc.Status{State: svc.StartPending}
		accepts := svc.AcceptShutdown | svc.AcceptStop
		changes <- svc.Status{State: svc.Running, Accepts: accepts}

		sensuAgent, err := agent.NewAgentContext(ctx, s.cfg)
		if err != nil {
			result <- err
			return
		}

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			defer cancel()
			logger.Info("signal received: ", <-sigs)
		}()

		go func() {
			if err := sensuAgent.Run(ctx); err != nil {
				result <- err
			}
		}()
	}()
	return result
}

func (s *Service) Execute(_ []string, r <-chan svc.ChangeRequest, changes chan<- svc.Status) (bool, uint32) {
	ctx, cancel := context.WithCancel(context.Background())
	errs := s.start(ctx, cancel, changes)
	elog, _ := eventlog.Open(serviceName)
	defer elog.Close()
	for {
		select {
		case req := <-r:
			switch req.Cmd {
			case svc.Stop, svc.Shutdown:
				elog.Info(1, "service shutting down")
				changes <- svc.Status{State: svc.StopPending}
				cancel()
				s.wg.Wait()
				changes <- svc.Status{State: svc.Stopped}
				return false, 0
			}
		case err := <-errs:
			elog.Error(1, fmt.Sprintf("restarting due to error: %s", err))
			s.start(ctx, cancel, changes)
		}
	}
	return false, 0
}
