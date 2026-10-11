package cos

import (
	"context"
	"errors"
	"log/slog"
)

// logErr logs err at level, or at debug when it only reports a cancelled ctx (shutdown, a closed client).
func logErr(ctx context.Context, level slog.Level, msg string, err error, args ...any) {
	if errors.Is(err, context.Canceled) || ctx.Err() != nil {
		level = slog.LevelDebug
	}
	slog.Log(context.Background(), level, msg, append(args, "error", err)...)
}
