package tailswarm

import (
	"context"
	"errors"
	"fmt"

	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	networktypes "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/swarm"
	"github.com/docker/docker/client"
)

// dockerAPI is the subset of *client.Client tailswarm uses. Pulling it
// out as an interface lets the wrapper be unit-tested without spinning
// up a real engine, and documents the docker-socket-proxy surface from
// DESIGN.md §8.
type dockerAPI interface {
	ServiceList(ctx context.Context, opts swarm.ServiceListOptions) ([]swarm.Service, error)
	ServiceInspectWithRaw(ctx context.Context, serviceID string, opts swarm.ServiceInspectOptions) (swarm.Service, []byte, error)
	ServiceCreate(ctx context.Context, spec swarm.ServiceSpec, opts swarm.ServiceCreateOptions) (swarm.ServiceCreateResponse, error)
	ServiceUpdate(ctx context.Context, serviceID string, version swarm.Version, spec swarm.ServiceSpec, opts swarm.ServiceUpdateOptions) (swarm.ServiceUpdateResponse, error)
	ServiceRemove(ctx context.Context, serviceID string) error
	NetworkList(ctx context.Context, opts networktypes.ListOptions) ([]networktypes.Summary, error)
	Events(ctx context.Context, opts events.ListOptions) (<-chan events.Message, <-chan error)
	Close() error
}

// Docker is a concrete DockerClient backed by the docker SDK. It also
// satisfies EventStream, so a single dependency drives both the
// reconciler and the watcher.
type Docker struct {
	api dockerAPI
}

// NewDocker constructs a Docker client honoring DOCKER_HOST and the
// other client.FromEnv knobs. The docker-socket-proxy in DESIGN.md §8
// works because the SDK respects DOCKER_HOST=tcp://docker-proxy:2375.
func NewDocker() (*Docker, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("docker: new client: %w", err)
	}
	return &Docker{api: cli}, nil
}

// Close releases the underlying docker client connection.
func (d *Docker) Close() error {
	if d.api == nil {
		return nil
	}
	return d.api.Close()
}

// Compile-time checks.
var (
	_ DockerClient = (*Docker)(nil)
	_ EventStream  = (*Docker)(nil)
)

// ListServices returns every service matching filter. An empty filter
// returns every service.
func (d *Docker) ListServices(ctx context.Context, filter LabelFilter) ([]swarm.Service, error) {
	opts := swarm.ServiceListOptions{}
	if filter.Key != "" {
		args := filters.NewArgs()
		if filter.Value != "" {
			args.Add("label", filter.Key+"="+filter.Value)
		} else {
			args.Add("label", filter.Key)
		}
		opts.Filters = args
	}
	return d.api.ServiceList(ctx, opts)
}

// InspectService returns the live service or ErrServiceNotFound when
// the engine reports a 404. Anything else is wrapped verbatim.
func (d *Docker) InspectService(ctx context.Context, serviceID string) (swarm.Service, error) {
	svc, _, err := d.api.ServiceInspectWithRaw(ctx, serviceID, swarm.ServiceInspectOptions{})
	if err != nil {
		if client.IsErrNotFound(err) {
			return swarm.Service{}, ErrServiceNotFound
		}
		return swarm.Service{}, err
	}
	return svc, nil
}

// CreateService translates a planner SidecarSpec into a swarm.ServiceSpec
// and creates it. The returned ID is what gets stored in Entry.SidecarID.
func (d *Docker) CreateService(ctx context.Context, spec SidecarSpec) (string, error) {
	resp, err := d.api.ServiceCreate(ctx, sidecarToServiceSpec(spec), swarm.ServiceCreateOptions{})
	if err != nil {
		return "", err
	}
	return resp.ID, nil
}

// UpdateService re-applies a SidecarSpec at the given version.
func (d *Docker) UpdateService(ctx context.Context, serviceID string, version uint64, spec SidecarSpec) error {
	_, err := d.api.ServiceUpdate(ctx, serviceID, swarm.Version{Index: version}, sidecarToServiceSpec(spec), swarm.ServiceUpdateOptions{})
	return err
}

// RemoveService deletes serviceID. A 404 is treated as success because
// the desired state is "gone".
func (d *Docker) RemoveService(ctx context.Context, serviceID string) error {
	if err := d.api.ServiceRemove(ctx, serviceID); err != nil {
		if client.IsErrNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

// ListNetworks returns swarm-scoped networks. The reconciler matches by
// Name, so we copy the list-summary fields it needs into swarm.Network.
func (d *Docker) ListNetworks(ctx context.Context) ([]swarm.Network, error) {
	summaries, err := d.api.NetworkList(ctx, networktypes.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]swarm.Network, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, swarm.Network{
			ID: s.ID,
			Spec: swarm.NetworkSpec{
				Annotations: swarm.Annotations{
					Name:   s.Name,
					Labels: s.Labels,
				},
				Ingress: s.Ingress,
			},
			DriverState: swarm.Driver{Name: s.Driver},
		})
	}
	return out, nil
}

// Subscribe satisfies EventStream. Service-scoped events arrive on the
// returned channel; the goroutine merges the SDK's separate message and
// error channels into a single typed stream and closes the channel when
// the underlying stream ends, so the watcher's resubscribe logic kicks
// in.
func (d *Docker) Subscribe(ctx context.Context) (<-chan Event, error) {
	args := filters.NewArgs()
	args.Add("type", string(events.ServiceEventType))
	msgs, errs := d.api.Events(ctx, events.ListOptions{Filters: args})

	out := make(chan Event, 16)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					// Drain any pending error so the SDK's goroutine can exit.
					select {
					case <-errs:
					default:
					}
					return
				}
				if msg.Type != events.ServiceEventType {
					continue
				}
				id := msg.Actor.ID
				if id == "" {
					id = msg.ID
				}
				if id == "" {
					continue
				}
				select {
				case out <- Event{ServiceID: id, Action: string(msg.Action)}:
				case <-ctx.Done():
					return
				}
			case err, ok := <-errs:
				if !ok {
					return
				}
				if err != nil && !errors.Is(err, context.Canceled) {
					// The watcher logs at the resubscribe point; we just
					// close to signal the stream is dead.
				}
				return
			}
		}
	}()
	return out, nil
}

// sidecarToServiceSpec is the translation seam from the planner's pure
// SidecarSpec to the Swarm SDK's nested ServiceSpec.
//
// Notes per DESIGN.md §4.2:
//   - One replica, no placement constraints.
//   - Labels go on the service (so resync's filter sees them) and on the
//     container (mirroring traefik's convention; cheap and unambiguous).
//   - Devices on the planner spec do not have a clean Swarm-API
//     equivalent — Swarm services don't expose a Devices field on
//     ContainerSpec. v1 relies on /dev/net/tun being made available
//     to the sidecar through node-level configuration (DESIGN.md §2);
//     we forward CapAdd which is the part Swarm does support.
func sidecarToServiceSpec(s SidecarSpec) swarm.ServiceSpec {
	envSlice := make([]string, 0, len(s.Env))
	for k, v := range s.Env {
		envSlice = append(envSlice, k+"="+v)
	}
	replicas := s.Replicas
	if replicas == 0 {
		replicas = 1
	}
	return swarm.ServiceSpec{
		Annotations: swarm.Annotations{
			Name:   s.Name,
			Labels: s.Labels,
		},
		TaskTemplate: swarm.TaskSpec{
			ContainerSpec: &swarm.ContainerSpec{
				Image:         s.Image,
				Hostname:      s.Hostname,
				Env:           envSlice,
				Labels:        s.Labels,
				CapabilityAdd: s.CapAdd,
			},
			Networks: []swarm.NetworkAttachmentConfig{{Target: s.NetworkID}},
		},
		Mode: swarm.ServiceMode{
			Replicated: &swarm.ReplicatedService{Replicas: &replicas},
		},
	}
}
