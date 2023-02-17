package coresync

import "context"

type Builder[S, T any] interface {
	SetRecorder(sRecorder RecordKey[S], tRecorder RecordKey[T]) ObjectDBSetter[S, T]
}

type ObjectDBSetter[S, T any] interface {
	SetSearcher(getter ObjectSearcher[S, T]) InformerBuilder[S, T]
}

type InformerBuilder[S, T any] interface {
	SetBilateralStreamer([]Informer[S], []Informer[T]) BilateralOperatorBuilder[S, T]
	SetUpstreamStreamer(...Informer[S]) UpstreamOperatorBuilder[S, T]
	SetDownstreamStreamer(...Informer[T]) DownstreamOperatorBuilder[S, T]
}

type BilateralOperatorBuilder[S, T any] interface {
	SetBilateralOperator(up []UpstreamTrigger[S, T], down []DownstreamTrigger[S, T]) SynchronizerFactory[S, T]
}

type UpstreamOperatorBuilder[S, T any] interface {
	SetUpstreamOperator(...UpstreamTrigger[S, T]) SynchronizerFactory[S, T]
}

type DownstreamOperatorBuilder[S, T any] interface {
	SetDownstreamOperator(...DownstreamTrigger[S, T]) SynchronizerFactory[S, T]
}

type SynchronizerFactory[S, T any] interface {
	CreateSynchronizer(opts ...Option[S, T]) (Synchronizer[S, T], error)
}

type ObjectSearcher[S, T any] interface {
	GetTarget(target *T) (*T, bool)
	ListTarget() []*T
	GetSource(source *S) (*S, bool)
	ListSource() []*S
}

type Informer[T any] interface {
	InflowChannel() CreationStreamChannel[T]
	UpdateChannel() UpdatingStreamChannel[T]
	OutflowChannel() CreationStreamChannel[T]
}

type DownstreamTrigger[S, T any] interface {
	DownstreamInflow(source *S, inflow *T, rest, missing []*T) error
	DownstreamUpdate(source *S, update *ResourceUpdater[T], rest, missing []*T) error
	DownstreamOutflow(source *S, outflow *T, rest, missing []*T) error
}

type UpstreamTrigger[S, T any] interface {
	UpstreamInflow(source *S, rest, missing []*T) error
	UpstreamUpdate(source *ResourceUpdater[S], rest, missing []*T) error
	UpstreamOutflow(source *S, rest, missing []*T) error
}

type Synchronizer[S, T any] interface {
	Run(ctx context.Context)
	CreateBind(source *S, target *T) error
	DeleteBind(source *S, targets ...*T)
	ListSyncUpstream() map[string]*S
	ListSyncDownstream() map[string]*T
	GetSyncDownstream(source *S) (map[string]*T, bool)
	GetSyncUpstream(target *T) (*S, bool)
}
