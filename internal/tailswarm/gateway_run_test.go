package tailswarm

import (
	"context"
	"errors"
	"testing"
)

// envMap returns an os.Getenv-shaped lookup over a fixed map, so RunGateway
// can be driven without touching the process environment.
func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// RunGateway must reject a missing/blank target set before it ever tries to
// bring up a tsnet.Server, so a misconfigured gateway fails fast in logs
// rather than registering a useless node.
func TestRunGatewayEmptyTargets(t *testing.T) {
	err := RunGateway(context.Background(), envMap(map[string]string{
		envGatewayHostname: "gw-tester",
		envGatewayTag:      "tag:svc-tester",
	}), nil)
	if err == nil {
		t.Fatal("expected error for empty targets, got nil")
	}
}

// A malformed targets entry surfaces the same parse error the label parser
// uses, wrapped under the gateway prefix.
func TestRunGatewayBadTargets(t *testing.T) {
	err := RunGateway(context.Background(), envMap(map[string]string{
		envGatewayHostname: "gw-tester",
		envGatewayTargets:  "db-mysql",
	}), nil)
	if !errors.Is(err, ErrBadEgressTarget) {
		t.Fatalf("err = %v, want ErrBadEgressTarget", err)
	}
}
