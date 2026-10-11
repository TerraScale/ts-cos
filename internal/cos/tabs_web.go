package cos

import (
	"log/slog"
	"net/http"
	"net/url"
	"strings"
)

// openTab resolves ?tab= to a chat tab; ok is false for unknown or closed tabs.
func (w *Web) openTab(r *http.Request) (ChatTab, bool) {
	id := r.URL.Query().Get("tab")
	if id == "" || id == chatMainTab {
		return mainTab, true
	}
	t, ok := w.Chat.Tab(id)
	if !ok {
		return ChatTab{ID: id}, false
	}
	return t, !t.Closed
}

// openTabs keeps the tabs that are not closed.
func openTabs(all []ChatTab) []ChatTab {
	out := []ChatTab{}
	for _, t := range all {
		if !t.Closed {
			out = append(out, t)
		}
	}
	return out
}

func tabHref(id string) string {
	if id == chatMainTab {
		return "/"
	}
	return "/?tab=" + url.QueryEscape(id)
}

// tabAction is the form action for one tab operation, such as rename or close.
func tabAction(id, verb string) string {
	return "/tabs/" + url.PathEscape(id) + "/" + verb
}

func chatEventsAction(tab string) string {
	return "@get('/chat/events?tab=" + url.QueryEscape(tab) + "', {retry: 'always', retryMaxCount: Infinity, retryMaxWait: 10000})"
}

func chatPostAction(tab string) string {
	return "@post('/chat?tab=" + url.QueryEscape(tab) + "')"
}

// composerPlaceholder names the tab the composer talks to.
func composerPlaceholder(t ChatTab) string {
	return "Message " + t.Name
}

// tabRedirect answers a tab form post with 303 to the chat page for tab; a non-empty notice shows there.
func tabRedirect(rw http.ResponseWriter, r *http.Request, tab, notice string) {
	var query []string
	if tab != "" {
		query = append(query, "tab="+url.QueryEscape(tab))
	}
	if notice != "" {
		query = append(query, "notice="+url.QueryEscape(notice))
	}
	target := "/"
	if len(query) > 0 {
		target += "?" + strings.Join(query, "&")
	}
	http.Redirect(rw, r, target, http.StatusSeeOther)
}

// createTab opens a new chat tab from the New tab dialog.
func (w *Web) createTab(rw http.ResponseWriter, r *http.Request) {
	t, err := w.Chat.CreateTab(r.Context(), r.FormValue("name"), r.FormValue("focus"))
	if err != nil {
		tabRedirect(rw, r, r.FormValue("tab"), err.Error())
		return
	}
	tabRedirect(rw, r, t.ID, "")
}

// renameTab changes an open tab's name; the COS tab keeps its name.
func (w *Web) renameTab(rw http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == chatMainTab {
		tabRedirect(rw, r, id, "The COS tab cannot be renamed")
		return
	}
	t, ok := w.Chat.Tab(id)
	if !ok || t.Closed {
		tabRedirect(rw, r, "", "This tab is closed")
		return
	}
	t.Name = strings.TrimSpace(r.FormValue("name"))
	if err := validTab(t); err != nil {
		tabRedirect(rw, r, id, err.Error())
		return
	}
	if err := w.Chat.UpdateTab(r.Context(), t); err != nil {
		slog.Warn("rename chat tab", "tab", id, "error", err)
		tabRedirect(rw, r, id, "The tab changed meanwhile; try again")
		return
	}
	tabRedirect(rw, r, id, "")
}

// closeTab marks an open tab closed; its COS thread is settled by the leader.
func (w *Web) closeTab(rw http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == chatMainTab {
		tabRedirect(rw, r, id, "The COS tab cannot be closed")
		return
	}
	t, ok := w.Chat.Tab(id)
	if !ok || t.Closed {
		tabRedirect(rw, r, "", "This tab is already closed")
		return
	}
	t.Closed = true
	if err := w.Chat.UpdateTab(r.Context(), t); err != nil {
		slog.Warn("close chat tab", "tab", id, "error", err)
		tabRedirect(rw, r, id, "The tab changed meanwhile; try again")
		return
	}
	tabRedirect(rw, r, "", "")
}
