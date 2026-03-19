package statemachine

import (
	"context"
	"errors"
	"strings"

	"github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/config"
	repository "github.com/alipourhabibi/raft/internal/repository/statemachine"
)

type StateMachine interface {
	Apply(ctx context.Context, entry *raft.Entry) error
}

type stateMachine struct {
	config     *config.Config
	repository repository.StateMachineRepo
}

func NewStateMachine(
	config *config.Config,
	repository repository.StateMachineRepo,
) (StateMachine, error) {
	return &stateMachine{
		config:     config,
		repository: repository,
	}, nil
}

func (s *stateMachine) Apply(ctx context.Context, entry *raft.Entry) error {
	command := strings.Split(entry.Command, " ")
	switch strings.ToLower(command[0]) {
	case "set":
		if len(command) < 3 {
			return errors.New("invalid command")
		}
		return s.repository.Set(ctx, command[1], command[2])
	}
	return nil
}
