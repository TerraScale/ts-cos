// Package behavior holds COS's versioned behavior: the agent brief, rules text,
// wake budget and chat message templates. A change merged to main reaches every
// node through NATS in seconds without a rebuild; the embedded copy is the
// fallback until the cluster has published one.
package behavior

import "embed"

//go:embed brief.md rules.md wake.json models.json templates
var Files embed.FS
