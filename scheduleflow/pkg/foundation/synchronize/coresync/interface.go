package coresync

import "context"

type Builder[S, T any] interface {
	SetBaseInformation(sRecorder RecordKey[S], tRecorder RecordKey[T], getter Getter[S, T]) InformerBuilder[S, T]
}

type InformerBuilder[S, T any] interface {
	SetBilateralStreamer(Informer[S], Informer[T]) BilateralOperatorBuilder[S, T]
	SetUpstreamStreamer(Informer[S]) UpstreamOperatorBuilder[S, T]
	SetDownstreamStreamer(Informer[T]) DownstreamOperatorBuilder[S, T]
}

type BilateralOperatorBuilder[S, T any] interface {
	SetBilateralOperator(UpstreamOperator[S, T], DownstreamOperator[S, T], ...SynchronizerOption[S, T]) Synchronizer[S, T]
}

type UpstreamOperatorBuilder[S, T any] interface {
	SetUpstreamOperator(UpstreamOperator[S, T], ...SynchronizerOption[S, T]) Synchronizer[S, T]
}

type DownstreamOperatorBuilder[S, T any] interface {
	SetDownstreamOperator(DownstreamOperator[S, T], ...SynchronizerOption[S, T]) Synchronizer[S, T]
}

type Getter[S, T any] interface {
	GetTarget(target *T) (*T, bool)
	ListTarget(target *T) []*T
	GetSource(source *S) (*S, bool)
	ListSource(source *S) []*S
}

type Informer[T any] interface {
	InflowChannel() CreationStreamChannel[T]
	UpdateChannel() UpdatingStreamChannel[T]
	OutflowChannel() CreationStreamChannel[T]
}

type DownstreamOperator[S, T any] interface {
	DownstreamInflow(source *S, inflows *T, rest, missing []*T) error
	DownstreamUpdate(source *S, updates *ResourceUpdater[T], rest, missing []*T) error
	DownstreamOutflow(source *S, outflows *T, rest, missing []*T) error
}

type UpstreamOperator[S, T any] interface {
	UpstreamInflow(source *S, rest, missing []*T) error
	UpstreamUpdate(source *ResourceUpdater[S], rest, missing []*T) error
	UpstreamOutflow(source *S, rest, missing []*T) error
}

type Synchronizer[S, T any] interface {
	Run(ctx context.Context)
	CreateBind(source *S, targets ...*T) error
	ListSyncUpstream() map[string]*S
	ListSyncDownstream() map[string]*T
	GetSyncDownstream(source *S) (map[string]*T, bool)
	GetSyncUpstream(target *T) (*S, bool)
	DeleteBind(source *S, targets ...*T)
}
