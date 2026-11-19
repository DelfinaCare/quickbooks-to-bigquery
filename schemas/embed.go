// Package schemas embeds the declarative YAML table definitions into the binary.
package schemas

import "embed"

// FS holds every schemas/*.yaml file.
//
//go:embed *.yaml
var FS embed.FS
