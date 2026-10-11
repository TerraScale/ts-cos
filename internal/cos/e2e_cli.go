package cos

func init() {
	cliCommands["e2e"] = cliCommand{Usage: "e2e [--smoke] [check-id-prefix...]", Run: E2E}
}
