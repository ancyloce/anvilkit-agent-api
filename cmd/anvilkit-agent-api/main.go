// anvilkit-agent-api is the public REST/SSE entry: authentication, OpenAPI
// validation, command mapping, authorized queries and SSE. It holds no
// business database, Temporal, Pagix or provider client.
package main

import (
	"go.uber.org/fx"

	"github.com/ancyloce/anvilkit-agent-api/internal/bootstrap"
)

func main() {
	fx.New(bootstrap.Module()).Run()
}
