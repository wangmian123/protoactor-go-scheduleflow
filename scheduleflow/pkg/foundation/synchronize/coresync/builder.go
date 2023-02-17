package coresync

import (
	"fmt"
)

type builder[S, T any] struct {
}

func (b *builder[S, T]) SetRecorder(upRecorder RecordKey[S], downRecorder RecordKey[T]) ObjectDBSetter[S, T] {
	return &objectSearcher[S, T]{
		upRecorder:   upRecorder,
		downRecorder: downRecorder,
	}
}

type objectSearcher[S, T any] struct {
	upRecorder   RecordKey[S]
	downRecorder RecordKey[T]
}

func (b *objectSearcher[S, T]) SetSearcher(getter ObjectSearcher[S, T]) InformerBuilder[S, T] {
	return &streamerSetter[S, T]{
		upRecorder:   b.upRecorder,
		downRecorder: b.downRecorder,
		getter:       getter,
	}
}

type streamerSetter[S, T any] struct {
	upRecorder   RecordKey[S]
	downRecorder RecordKey[T]
	getter       ObjectSearcher[S, T]
}

func (s *streamerSetter[S, T]) SetBilateralStreamer(up []Informer[S], down []Informer[T]) BilateralOperatorBuilder[S, T] {
	return &operatorBuilder[S, T]{
		upstreamInformers:   up,
		downstreamInformers: down,
		upRecorder:          s.upRecorder,
		downRecorder:        s.downRecorder,
		getter:              s.getter,
	}
}

func (s *streamerSetter[S, T]) SetUpstreamStreamer(up ...Informer[S]) UpstreamOperatorBuilder[S, T] {
	return &operatorBuilder[S, T]{
		upstreamInformers: up,
		upRecorder:        s.upRecorder,
		downRecorder:      s.downRecorder,
		getter:            s.getter,
	}
}

func (s *streamerSetter[S, T]) SetDownstreamStreamer(down ...Informer[T]) DownstreamOperatorBuilder[S, T] {
	return &operatorBuilder[S, T]{
		downstreamInformers: down,
		upRecorder:          s.upRecorder,
		downRecorder:        s.downRecorder,
		getter:              s.getter,
	}
}

type operatorBuilder[S, T any] struct {
	upRecorder          RecordKey[S]
	downRecorder        RecordKey[T]
	getter              ObjectSearcher[S, T]
	upstreamInformers   []Informer[S]
	downstreamInformers []Informer[T]
}

func (ope *operatorBuilder[S, T]) SetBilateralOperator(up []UpstreamTrigger[S, T], downs []DownstreamTrigger[S, T]) SynchronizerFactory[S, T] {
	return &synchronizerFactory[S, T]{
		upstreamOperators:   up,
		upstreamInformers:   ope.upstreamInformers,
		upRecorder:          ope.upRecorder,
		downRecorder:        ope.downRecorder,
		getter:              ope.getter,
		downstreamInformers: ope.downstreamInformers,
		downstreamOperators: downs,
		sType:               BilateralSync,
	}
}

func (ope *operatorBuilder[S, T]) SetUpstreamOperator(up ...UpstreamTrigger[S, T]) SynchronizerFactory[S, T] {
	return &synchronizerFactory[S, T]{
		upstreamOperators: up,
		upstreamInformers: ope.upstreamInformers,
		upRecorder:        ope.upRecorder,
		downRecorder:      ope.downRecorder,
		getter:            ope.getter,
		sType:             UpstreamSync,
	}
}

func (ope *operatorBuilder[S, T]) SetDownstreamOperator(downs ...DownstreamTrigger[S, T]) SynchronizerFactory[S, T] {
	return &synchronizerFactory[S, T]{
		upRecorder:          ope.upRecorder,
		downRecorder:        ope.downRecorder,
		getter:              ope.getter,
		downstreamInformers: ope.downstreamInformers,
		downstreamOperators: downs,
		sType:               DownstreamSync,
	}
}

type synchronizerFactory[S, T any] struct {
	upRecorder          RecordKey[S]
	downRecorder        RecordKey[T]
	getter              ObjectSearcher[S, T]
	upstreamInformers   []Informer[S]
	downstreamInformers []Informer[T]
	upstreamOperators   []UpstreamTrigger[S, T]
	downstreamOperators []DownstreamTrigger[S, T]
	sType               SynchronizerType
}

func (fac *synchronizerFactory[S, T]) CreateSynchronizer(opts ...Option[S, T]) (Synchronizer[S, T], error) {
	switch fac.sType {
	case BilateralSync:
		return fac.createBilateralSynchronizer(opts...)
	case UpstreamSync:
		return fac.createUpstreamSynchronizer(opts...)
	case DownstreamSync:
		return fac.createDownstreamSynchronizer(opts...)
	default:
		return nil, fmt.Errorf("synchronizer type error")
	}
}

func (fac *synchronizerFactory[S, T]) createBilateralSynchronizer(opts ...Option[S, T]) (Synchronizer[S, T], error) {
	o := newOption[S, T]()
	for _, opt := range opts {
		opt(o)
	}

	if err := fac.checkUpstreamArguments(); err != nil {
		return nil, err
	}
	if err := fac.checkDownstreamArguments(); err != nil {
		return nil, err
	}

	throughput := synchronizerFlux[S, T]{
		upstreamInformers:   fac.upstreamInformers,
		downstreamInformers: fac.downstreamInformers,
		upstreamOperators:   fac.upstreamOperators,
		downstreamOperators: fac.downstreamOperators,
		getter:              fac.getter,
	}
	record := recorder[S, T]{upstreamRecorder: fac.upRecorder, downstreamRecorder: fac.downRecorder}
	return newSynchronizer[S, T](throughput, record, o, o), nil
}

func (fac *synchronizerFactory[S, T]) createUpstreamSynchronizer(opts ...Option[S, T]) (Synchronizer[S, T], error) {
	o := newOption[S, T]()
	for _, opt := range opts {
		opt(o)
	}

	if err := fac.checkUpstreamArguments(); err != nil {
		return nil, err
	}

	throughput := synchronizerFlux[S, T]{
		upstreamInformers: fac.upstreamInformers,
		upstreamOperators: fac.upstreamOperators,
		getter:            fac.getter,
	}
	record := recorder[S, T]{upstreamRecorder: fac.upRecorder, downstreamRecorder: fac.downRecorder}
	return newSynchronizer[S, T](throughput, record, o, nil), nil
}

func (fac *synchronizerFactory[S, T]) createDownstreamSynchronizer(opts ...Option[S, T]) (Synchronizer[S, T], error) {
	o := newOption[S, T]()
	for _, opt := range opts {
		opt(o)
	}

	if err := fac.checkDownstreamArguments(); err != nil {
		return nil, err
	}

	throughput := synchronizerFlux[S, T]{
		downstreamInformers: fac.downstreamInformers,
		downstreamOperators: fac.downstreamOperators,
		getter:              fac.getter,
	}
	record := recorder[S, T]{upstreamRecorder: fac.upRecorder, downstreamRecorder: fac.downRecorder}
	return newSynchronizer[S, T](throughput, record, nil, o), nil
}

func (fac *synchronizerFactory[S, T]) checkUpstreamArguments() error {
	errParse := "%s should not be nil"
	if fac.getter == nil {
		return fmt.Errorf(errParse, "ObjectSearcher")
	}

	if fac.upRecorder == nil {
		return fmt.Errorf(errParse, "UpstreamRecorder")
	}
	if fac.downRecorder == nil {
		return fmt.Errorf(errParse, "DownstreamRecorder")
	}

	if fac.upstreamInformers == nil {
		return fmt.Errorf(errParse, "UpstreamInformer")
	}

	if fac.upstreamOperators == nil {
		return fmt.Errorf(errParse, "UpstreamOpeartor")
	}

	return nil
}

func (fac *synchronizerFactory[S, T]) checkDownstreamArguments() error {
	errParse := "%s should not be nil"
	if fac.getter == nil {
		return fmt.Errorf(errParse, "ObjectSearcher")
	}

	if fac.upRecorder == nil {
		return fmt.Errorf(errParse, "UpstreamRecorder")
	}
	if fac.downRecorder == nil {
		return fmt.Errorf(errParse, "DownstreamRecorder")
	}

	if fac.downstreamInformers == nil {
		return fmt.Errorf(errParse, "DownstreamInformers")
	}

	for i, inf := range fac.downstreamInformers {
		if inf == nil {
			return fmt.Errorf(errParse, fmt.Sprintf("DownstreamInformer-%d", i))
		}
	}

	if fac.downstreamOperators == nil {
		return fmt.Errorf(errParse, "DownstreamOperators")
	}

	for i, ope := range fac.downstreamOperators {
		if ope == nil {
			return fmt.Errorf(errParse, fmt.Sprintf("DownstreamTrigger-%d", i))
		}
	}

	return nil
}

func NewBuilder[S, T any]() Builder[S, T] {
	return &builder[S, T]{}
}
