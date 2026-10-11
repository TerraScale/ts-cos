package cos

import (
	"context"
	"errors"
	"io"
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
		var ok bool
		switch {
		case ticketPattern.FindString(args[0]) == args[0]:
			ticket = strings.ToUpper(args[0])
		default:
			if stage, ok = boardStageArg(args[0]); !ok {
				return usage
			}
		}
	}
	s, err := stateStore(cfg)
	if err != nil {
		return err
	}
	defer s.Close()
	data, err := loadBoard(ctx, s)
	if err != nil {
		return err
	}
	rows := data.Rows
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

// boardData is everything the Work board reads from the local store; the CLI and the page share it.
type boardData struct {
	Rows  []BoardRow
	Live  Live
	Usage []UsageRow // the last seven days
}

// loadBoard reads SQLite only, so a page view makes no T3 or Forgejo calls.
func loadBoard(ctx context.Context, s *Store) (boardData, error) {
	now := time.Now()
	tasks, err := s.allTasks(ctx)
	if err != nil {
		return boardData{}, err
	}
	live, err := s.Live(ctx)
	if err != nil {
		return boardData{}, err
	}
	open, _, err := inboxFor(ctx, s)
	if err != nil {
		return boardData{}, err
	}
	spans, deployedAt, err := s.LatestEvents(ctx)
	if err != nil {
		return boardData{}, err
	}
	usage, err := s.Usage(ctx, now.Add(-boardRecent).UTC().Format("2006-01-02"))
	if err != nil {
		return boardData{}, err
	}
	return boardData{Rows: BuildBoard(tasks, live, open, spans, deployedAt, now), Live: live, Usage: usage}, nil
}
