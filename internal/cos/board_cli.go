package cos

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"time"
)

const boardTimelineMax = 200

func init() {
	cliCommands["board"] = cliCommand{Usage: "board [queued | working | in-review | merging | deployed | done | <TICKET>]", Run: boardCommand}
}

func boardCommand(ctx context.Context, cfg Config, args []string, w io.Writer) error {
	usage := errors.New("usage: cos board [queued | working | in-review | merging | deployed | done | <TICKET>]")
	if len(args) > 1 {
		return usage
	}
	stage := ""
	ticket := ""
	if len(args) == 1 {
		switch {
		case ticketPattern.FindString(args[0]) == args[0]:
			ticket = strings.ToUpper(args[0])
		case args[0] == "review":
			stage = "in review"
		default:
			stage = strings.ReplaceAll(args[0], "-", " ")
			if !slices.Contains(boardStages, stage) {
				return usage
			}
		}
	}
	s, err := stateStore(cfg)
	if err != nil {
		return err
	}
	defer s.Close()
	rows, err := loadBoard(ctx, s)
	if err != nil {
		return err
	}
	if ticket != "" {
		row := BoardRow{Ticket: ticket}
		for _, r := range rows {
			if r.Ticket == ticket {
				row = r
			}
		}
		timeline, err := s.TicketEvents(ctx, ticket, boardTimelineMax)
		if err != nil {
			return err
		}
		return output(w, map[string]any{"row": row, "timeline": timeline})
	}
	out := []BoardRow{}
	for _, r := range rows {
		if stage == "" || r.Stage == stage {
			out = append(out, r)
		}
	}
	return output(w, map[string]any{"rows": out})
}

func loadBoard(ctx context.Context, s *Store) ([]BoardRow, error) {
	tasks, err := s.allTasks(ctx)
	if err != nil {
		return nil, err
	}
	live, err := s.Live(ctx)
	if err != nil {
		return nil, err
	}
	open, _, err := s.Inbox(ctx)
	if err != nil {
		return nil, err
	}
	spans, deployedAt, err := s.LatestEvents(ctx)
	if err != nil {
		return nil, err
	}
	return BuildBoard(tasks, live, open, spans, deployedAt, time.Now()), nil
}
