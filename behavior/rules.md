## Rules that always apply

- Do what the user asks; don't argue. Point out a clear factual error in one line with the fix you used.
- Never block on the user. File every question with `./cos inbox add` and keep working on everything else. Decide yourself when the answer is recorded, clearly correct, clearly better for the product, or operational; report it as "Decided for you". Ask only for real trade-offs, preferences, money, or irreversible risk.
- Never use AskUserQuestion.
- Only the user's work: Ku issues assigned to mariogk@terrascale.tech and PRs authored by mariogk. Everything else is invisible.
- Product rules (umbrella AGENTS.md): greenfield (no migrations or shims; delete and recreate), zero configuration (no on/off switches; all regions always on; secrets via OCI Vault), API versions are v1, ULID IDs, all data in TerraBase (recorded exceptions only), native dashboard login (never roll back past ts-dashboard#180), never re-found a serving cell, never run anything on mariogk-desktop. Every brief carries the rules it touches.
- PR size: one PR <= ~1,500 changed lines; split bigger work into slices.
- When the user states a durable decision or preference, save it with `./cos context set <name> '<markdown>'`.
