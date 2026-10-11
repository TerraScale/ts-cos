// Package icons renders the dashboard's lucide icon set as inline, decorative
// SVG so icons inherit currentColor and never need an extra request.
package icons

import (
	"context"
	"io"
	"strconv"

	"github.com/a-h/templ"
)

const (
	svgOpen   = `<svg xmlns="http://www.w3.org/2000/svg" width="`
	svgHeight = `" height="`
	svgAttrs  = `" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">`
	svgClose  = `</svg>`
)

// svg writes the icon piecewise so rendering never builds the whole element
// as an intermediate string.
func svg(paths string, size int) templ.Component {
	return templ.ComponentFunc(func(_ context.Context, w io.Writer) error {
		s := strconv.Itoa(size)
		for _, part := range [...]string{svgOpen, s, svgHeight, s, svgAttrs, paths, svgClose} {
			if _, err := io.WriteString(w, part); err != nil {
				return err
			}
		}
		return nil
	})
}

// Eye renders the lucide Eye icon at size pixels.
func Eye(size int) templ.Component { return svg(eyePaths, size) }

const eyePaths = `<path d="M2.062 12.348a1 1 0 0 1 0-.696 10.75 10.75 0 0 1 19.876 0 1 1 0 0 1 0 .696 10.75 10.75 0 0 1-19.876 0"></path><circle cx="12" cy="12" r="3"></circle>`
