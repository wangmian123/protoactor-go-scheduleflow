package coresync

type builder[S, T any] struct {
}

func (b *builder[S, T]) SetBaseInformation(upRecorder RecordKey[S], downRecorder RecordKey[T], getter Getter[S, T]) InformerBuilder[S, T] {
	return &streamerSetter[S, T]{
		upRecorder:   upRecorder,
		downRecorder: downRecorder,
		getter:       getter,
	}
}

type streamerSetter[S, T any] struct {
	upRecorder   RecordKey[S]
	downRecorder RecordKey[T]
	getter       Getter[S, T]
}

func (s *streamerSetter[S, T]) SetBilateralStreamer(up Informer[S], down Informer[T]) BilateralOperatorBuilder[S, T] {
	return &operatorBuilder[S, T]{
		upstreamInformer:   up,
		downstreamInformer: down,
		streamerSetter:     s,
	}
}

func (s *streamerSetter[S, T]) SetUpstreamStreamer(up Informer[S]) UpstreamOperatorBuilder[S, T] {
	return &operatorBuilder[S, T]{
		upstreamInformer: up,
		streamerSetter:   s,
	}
}

func (s *streamerSetter[S, T]) SetDownstreamStreamer(down Informer[T]) DownstreamOperatorBuilder[S, T] {
	return &operatorBuilder[S, T]{
		downstreamInformer: down,
		streamerSetter:     s,
	}
}

type operatorBuilder[S, T any] struct {
	upstreamInformer   Informer[S]
	downstreamInformer Informer[T]

	*streamerSetter[S, T]
}

func (ope *operatorBuilder[S, T]) SetBilateralOperator(up UpstreamOperator[S, T], down DownstreamOperator[S, T], opts ...SynchronizerOption[S, T]) Synchronizer[S, T] {
	option := newOption[S, T]()
	for _, opt := range opts {
		opt(option)
	}
	return newSynchronizer[S, T](ope.upstreamInformer, ope.downstreamInformer, up, down, ope.getter, ope.upRecorder, ope.downRecorder, option.creatingRetry, option.updatingRetry)
}

func (ope *operatorBuilder[S, T]) SetUpstreamOperator(up UpstreamOperator[S, T], opts ...SynchronizerOption[S, T]) Synchronizer[S, T] {
	option := newOption[S, T]()
	for _, opt := range opts {
		opt(option)
	}
	return newSynchronizer[S, T](ope.upstreamInformer, nil, up, nil, ope.getter, ope.upRecorder, ope.downRecorder, option.creatingRetry, option.updatingRetry)
}

func (ope *operatorBuilder[S, T]) SetDownstreamOperator(down DownstreamOperator[S, T], opts ...SynchronizerOption[S, T]) Synchronizer[S, T] {
	option := newOption[S, T]()
	for _, opt := range opts {
		opt(option)
	}
	return newSynchronizer[S, T](nil, ope.downstreamInformer, nil, down, ope.getter, ope.upRecorder, ope.downRecorder, option.creatingRetry, option.updatingRetry)
}

func NewBuilder[S, T any]() Builder[S, T] {
	return &builder[S, T]{}
}
