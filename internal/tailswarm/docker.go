package tailswarm

import (
	"context"
	"fmt"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	networktypes "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/swarm"
	"github.com/docker/docker/client"
)

// dockerAPI is the subset of *client.Client tailswarm uses. With tsnet
// in process, only read paths plus the event stream are needed — the
// reconciler no longer creates, updates, or removes any Docker service.
type dockerAPI interface {
	ServiceList(ctx context.Context, opts swarm.ServiceListOptions) ([]swarm.Service, error)
	ServiceInspectWithRaw(ctx context.Context, serviceID string, opts swarm.ServiceInspectOptions) (swarm.Service, []byte, error)
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
// other client.FromEnv knobs. The docker-socket-proxy works because
// the SDK respects DOCKER_HOST=tcp://docker-proxy:2375.
func NewDocker() (*Docker, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("docker: new client: %w", err)
	}
	return &Docker{api: cli}, nil
}

func (d *Docker) Close() error {
	if d.api == nil {
		return nil
	}
	return d.api.Close()
}

var (
	_ DockerClient = (*Docker)(nil)
	_ EventStream  = (*Docker)(nil)
)

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

func (d *Docker) InspectService(ctx context.Context, serviceID string) (swarm.Service, error) {
	svc, _, err := d.api.ServiceInspectWithRaw(ctx, serviceID, swarm.ServiceInspectOptions{})
	if err != nil {
		if cerrdefs.IsNotFound(err) {
			return swarm.Service{}, ErrServiceNotFound
		}
		return swarm.Service{}, err
	}
	return svc, nil
}

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

// Subscribe satisfies EventStream.
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
					continue
				}
				select {
				case out <- Event{ServiceID: id, Action: string(msg.Action)}:
				case <-ctx.Done():
					return
				}
			case _, ok := <-errs:
				if !ok {
					return
				}
				return
			}
		}
	}()
	return out, nil
}
