package tailswarm

import (
	"context"
	"fmt"
	"strings"
)

// DiscoverGatewayImage resolves the image egress gateways should run by
// finding the daemon's own Swarm task and reading the image its service
// deploys. Egress gateways are tailswarm's own binary in "gateway" mode,
// so the right image is always whatever this daemon is itself running —
// there is no separate knob to keep in sync.
//
// hostname is the daemon container's hostname, which Swarm sets to the
// container ID (os.Hostname() inside a task). We match it against each
// task's resolved ContainerID, then inspect that task's service and return
// its ContainerSpec.Image — the digest Swarm pinned at deploy time, so
// gateways track the daemon's exact image rather than a floating tag.
//
// It returns an error if no task matches (e.g. the daemon isn't running as
// a Swarm service, or hostname was overridden) so egress fails loudly with
// a misconfiguration rather than silently deploying a wrong image.
func DiscoverGatewayImage(ctx context.Context, docker DockerClient, hostname string) (string, error) {
	if hostname == "" {
		return "", fmt.Errorf("tailswarm: cannot discover gateway image: empty hostname")
	}

	tasks, err := docker.ListTasks(ctx)
	if err != nil {
		return "", fmt.Errorf("list tasks: %w", err)
	}

	var serviceID string
	for _, t := range tasks {
		cs := t.Status.ContainerStatus
		if cs == nil || cs.ContainerID == "" {
			continue
		}
		// The container ID is the full 64-char digest; the hostname Swarm
		// injects is its short prefix. Match either direction so a full or
		// truncated hostname both resolve.
		if strings.HasPrefix(cs.ContainerID, hostname) || strings.HasPrefix(hostname, cs.ContainerID) {
			serviceID = t.ServiceID
			break
		}
	}
	if serviceID == "" {
		return "", fmt.Errorf("tailswarm: no Swarm task matches hostname %q; "+
			"the daemon must run as a Swarm service for egress gateway image discovery", hostname)
	}

	svc, err := docker.InspectService(ctx, serviceID)
	if err != nil {
		return "", fmt.Errorf("inspect own service %s: %w", serviceID, err)
	}
	if svc.Spec.TaskTemplate.ContainerSpec == nil || svc.Spec.TaskTemplate.ContainerSpec.Image == "" {
		return "", fmt.Errorf("tailswarm: own service %s has no container image", serviceID)
	}
	return svc.Spec.TaskTemplate.ContainerSpec.Image, nil
}
