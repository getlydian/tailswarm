package tailswarm

import (
	"context"
	"strings"
	"testing"

	"github.com/docker/docker/api/types/swarm"
)

// DiscoverGatewayImage finds the daemon's own task by matching the
// container hostname against task container IDs, then returns the image
// that task's service runs.
func TestDiscoverGatewayImageMatchesOwnTask(t *testing.T) {
	d := newFakeDocker()
	id := d.addService(swarm.Service{
		Spec: swarm.ServiceSpec{
			TaskTemplate: swarm.TaskSpec{
				ContainerSpec: &swarm.ContainerSpec{Image: "ghcr.io/getlydian/tailswarm@sha256:abc"},
			},
		},
	})
	d.tasks = []swarm.Task{
		{ServiceID: "other", Status: swarm.TaskStatus{ContainerStatus: &swarm.ContainerStatus{ContainerID: "ffff0000"}}},
		{ServiceID: id, Status: swarm.TaskStatus{ContainerStatus: &swarm.ContainerStatus{ContainerID: "abcdef123456789"}}},
	}

	// os.Hostname() in a Swarm task is the short container-ID prefix.
	img, err := DiscoverGatewayImage(context.Background(), d, "abcdef123456")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if img != "ghcr.io/getlydian/tailswarm@sha256:abc" {
		t.Fatalf("image = %q", img)
	}
}

// No matching task is an error, so egress fails loudly rather than
// deploying gateways with a wrong or empty image.
func TestDiscoverGatewayImageNoMatch(t *testing.T) {
	d := newFakeDocker()
	d.tasks = []swarm.Task{
		{ServiceID: "x", Status: swarm.TaskStatus{ContainerStatus: &swarm.ContainerStatus{ContainerID: "deadbeef"}}},
	}
	_, err := DiscoverGatewayImage(context.Background(), d, "abcdef123456")
	if err == nil || !strings.Contains(err.Error(), "no Swarm task matches") {
		t.Fatalf("err = %v, want no-match error", err)
	}
}

// An empty hostname can't be matched and is rejected up front.
func TestDiscoverGatewayImageEmptyHostname(t *testing.T) {
	_, err := DiscoverGatewayImage(context.Background(), newFakeDocker(), "")
	if err == nil {
		t.Fatal("expected error for empty hostname")
	}
}
