package flow

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"time"
)

// TODO - replace public facing types with interfaces
// TODO - all the comments

func App(name string) *AppBuilder {
	return &AppBuilder{
		name:  name,
		flows: make([]*flowBuilder, 0),
	}
}

func Flow[Tinit any](name string) *FlowBuilder[Tinit, Tinit] {
	return &FlowBuilder[Tinit, Tinit]{
		name:     name,
		triggerT: reflect.TypeFor[Tinit](),
		handlers: make([]*completedHandlerBuilder, 0),
	}
}

func Handler[Tin, Tout any](name string) *HandlerBuilder[Tin, Tout] {
	return &HandlerBuilder[Tin, Tout]{
		name:        name,
		maxAttempts: nil,
		retryPeriod: nil,
	}
}

func EndHandler[Tin any](name string) *EndHandlerBuilder[Tin] {
	return &EndHandlerBuilder[Tin]{
		name: name,
	}
}

func SplitHandler[Tin any](name string) *SplitHandlerBuilder[Tin] {
	return &SplitHandlerBuilder[Tin]{
		name: name,
	}
}

func Component(name string) *ComponentBuilder {
	return &ComponentBuilder{
		name: name,
	}
}

type NilType struct{}

type AppBuilder struct {
	name       string
	flows      []*flowBuilder
	components []*ComponentBuilder
	logger     *slog.Logger
}

func (a *AppBuilder) WithFlow[Tinit any](flow *FlowBuilder[Tinit, NilType]) *AppBuilder {
	a.flows = append(a.flows, (*flowBuilder)(flow))
	return a
}

func (a *AppBuilder) WithComponents(componentBuilders ...*ComponentBuilder) *AppBuilder {
	a.components = componentBuilders
	return a
}

func (a *AppBuilder) WithLogger(logger *slog.Logger) *AppBuilder {
	a.logger = logger
	return a
}

type defaultValueTracker struct {
	flows      *util.Set[string]
	handlers   *util.Set[string]
	components *util.Set[string]
}

func newDefaultValueTracker() *defaultValueTracker {
	return &defaultValueTracker{
		flows:      util.NewSet[string](),
		handlers:   util.NewSet[string](),
		components: util.NewSet[string](),
	}
}

type validationResult struct {
	errors []string
}

func newValidationResult() *validationResult {
	return &validationResult{
		errors: make([]string, 0),
	}
}

func (v *validationResult) addError(err string, args ...any) {
	v.errors = append(v.errors, fmt.Sprintf(err, args...))
}

func (v *validationResult) combine(result *validationResult) {
	v.errors = append(v.errors, result.errors...)
}

func (v *validationResult) ok() bool {
	return len(v.errors) == 0
}

func (v *validationResult) toError() error {
	errs := make([]error, len(v.errors))
	for i, e := range v.errors {
		errs[i] = errors.New(e)
	}

	return errors.Join(errs...)
}

func (a *AppBuilder) applyDefaults() {
	appliedTracker := newDefaultValueTracker()

	for _, v := range a.flows {
		v.applyDefaults(appliedTracker)
	}

	for _, v := range a.components {
		v.applyDefaults(appliedTracker)
	}

	if a.logger == nil {
		a.logger = slog.Default()
	}
}

func (a *AppBuilder) validate() *validationResult {
	r := newValidationResult()

	if len(a.flows) == 0 {
		r.addError("App %v must contain at least 1 flow", a.name)
	}

	hNames := util.NewSet[string]()
	complete := util.NewSet[string]()
	for _, v := range a.flows {
		inProg := util.NewSet[string]()
		r.combine(v.validate(hNames, inProg, complete))
	}

	if a.components != nil {
		for _, v := range a.components {
			r.combine(v.validate(hNames))
		}
	}

	return r
}

func (a *AppBuilder) Build() (*BuiltApp, error) {
	handlerMap := make(map[string]*handlerCoupling) // Map of handler names to couplings
	flowMap := make(map[string][]*handlerCoupling)  // Map of flow names to coupling collections
	inProg := util.NewSet[string]()
	ctx, cancel := context.WithCancel(context.Background())

	// Apply default values for anything we reasonably can if it hasn't been specified by the user
	a.applyDefaults()

	// Validate the app has a valid structure
	vRes := a.validate()
	if !vRes.ok() {
		cancel() // To avoid context leak
		return nil, vRes.toError()
	}

	// Build all the couplings between input pipes, output pipes and handlers for all flows
	for _, v := range a.flows {
		v.buildCouplings(a.logger, &flowMap, &handlerMap, inProg)
	}

	// Build components where multiple handlers have been grouped together, keeping track of which component each handler is in
	components := make([]*component, 0)
	componentMap := make(map[string]*component) // Map of handler names to components
	for _, v := range a.components {
		couplings := make([]*handlerCoupling, 0, len(v.handlers))
		for _, c := range v.handlers {
			coupling, ok := handlerMap[c]
			if !ok {
				err := fmt.Sprintf("Unable to find handler %v for component %v", c, v.name)
				panic(err)
			}
			couplings = append(couplings, coupling)
			delete(handlerMap, c)
		}

		component := newComponent(ctx, a.logger, couplings, *v.maxConcurrency)
		components = append(components, component)

		for _, c := range v.handlers {
			componentMap[c] = component
		}
	}

	// Build a single component per remaining handler
	for _, v := range handlerMap {
		component := newComponent(ctx, a.logger, []*handlerCoupling{v}, 1)
		components = append(components, component)
		componentMap[v.handler.name] = component
	}

	// Build the flow objects
	flows := make(map[string]*flow)
	for _, v := range a.flows {
		inputPipe := flowMap[v.name][0].inputPipe
		endComponent := componentMap[flowMap[v.name][len(flowMap[v.name])-1].handler.name]
		flow := &flow{
			name:         v.name,
			triggerT:     v.triggerT,
			inputPipe:    inputPipe,
			endComponent: endComponent,
		}
		flows[v.name] = flow
	}

	return &BuiltApp{
		name:       a.name,
		flows:      flows,
		components: components,
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

type flowBuilder struct {
	name           string
	triggerT       reflect.Type
	handlers       []*completedHandlerBuilder
	continuingFlow *flowBuilder
}

type FlowBuilder[Tinit, Tnext any] flowBuilder

func (fb *FlowBuilder[Tinit, Tnext]) WithHandler[Tout any](hb *CompletedHandlerBuilder[Tnext, Tout]) *FlowBuilder[Tinit, Tout] {
	fb.handlers = append(fb.handlers, (*completedHandlerBuilder)(hb))
	return fb.toNextBuilder[Tout]()
}

func (fb *FlowBuilder[Tinit, Tnext]) WithFlow(endingFb *FlowBuilder[Tnext, NilType]) *FlowBuilder[Tinit, NilType] {
	fb.continuingFlow = (*flowBuilder)(endingFb)
	return fb.toNextBuilder[NilType]()
}

func (fb *FlowBuilder[Tinit, Tnext]) toNextBuilder[Tnextnext any]() *FlowBuilder[Tinit, Tnextnext] {
	return &FlowBuilder[Tinit, Tnextnext]{
		name:           fb.name,
		triggerT:       fb.triggerT,
		handlers:       fb.handlers,
		continuingFlow: fb.continuingFlow,
	}
}

func (fb *flowBuilder) applyDefaults(tracker *defaultValueTracker) {
	if tracker.flows.Contains(fb.name) {
		return
	}
	tracker.flows.Add(fb.name)

	for _, v := range fb.handlers {
		v.applyDefaults(tracker)
	}

	if fb.continuingFlow != nil {
		fb.continuingFlow.applyDefaults(tracker)
	}
}

func (fb *flowBuilder) validate(handlerNames *util.Set[string], inProg *util.Set[string], complete *util.Set[string]) *validationResult {
	r := newValidationResult()
	if complete.Contains(fb.name) {
		// This flow has already been validated
		return r
	}

	if ok := inProg.Add(fb.name); !ok {
		// Circular dependency
		r.addError("Flow %v is in a circular dependency", fb.name)
		return r
	}

	if len(fb.handlers) == 0 {
		r.addError("Flow %v must contain at least 1 handler", fb.name)
	}

	for _, v := range fb.handlers {
		if len(v.name) == 0 {
			r.addError("Flow %v contains a handler with an empty name", fb.name)
		}

		if ok := handlerNames.Add(v.name); !ok {
			r.addError("Handler %v in flow %v has a duplicate name", v.name, fb.name)
		}

		if v.errorFlow != nil {
			r.combine(v.errorFlow.validate(handlerNames, inProg, complete))
		}

		if v.continuingFlows != nil {
			for _, w := range v.continuingFlows {
				r.combine(w.validate(handlerNames, inProg, complete))
			}
		}
	}

	complete.Add(fb.name)
	return r
}

// Only builds the association between input pipes, handlers and output pipes - building components is done further up the stack
func (fb *flowBuilder) buildCouplings(logger *slog.Logger, flowMap *map[string][]*handlerCoupling, handlerMap *map[string]*handlerCoupling, inProg *util.Set[string]) []*handlerCoupling {
	if f, ok := (*flowMap)[fb.name]; ok {
		// The flow's already been built, just return it
		return f
	}

	// Add this flow to the set of flows currently being built
	if !inProg.Add(fb.name) {
		// Circular reference
		err := fmt.Sprintf("Unable to build flow %v - circular reference", fb.name)
		panic(err)
	}

	couplings := make([]*handlerCoupling, 0)

	// Create all the input pipes and associate them with their handlers
	for _, v := range fb.handlers {
		var errPipe *pipe
		if v.errorFlow != nil {
			errPipe = v.errorFlow.buildCouplings(logger, flowMap, handlerMap, inProg)[0].inputPipe
		}
		retryPolicy := newRetryPolicy(errPipe, *v.maxAttempts, *v.retryPeriod)

		coupling := &handlerCoupling{
			handler:     v.build(),
			inputPipe:   newPipe(fmt.Sprintf("%v_input", v.name), v.batchPolicy, logger, fb.name, []string{}), // The following flow names are added later
			retryPolicy: retryPolicy,
		}
		(*handlerMap)[coupling.handler.name] = coupling
		couplings = append(couplings, coupling)
	}

	// If the flow is completed by another flow or a split handler, build those flows as well
	nextFlowPipes := make([]*pipe, 0)
	lastHandler := fb.handlers[len(fb.handlers)-1]
	if fb.continuingFlow != nil && len(lastHandler.continuingFlows) == 0 { // Flow is completed by a different flow
		nextFlowPipes = append(nextFlowPipes, fb.continuingFlow.buildCouplings(logger, flowMap, handlerMap, inProg)[0].inputPipe)
	} else if fb.continuingFlow == nil && len(lastHandler.continuingFlows) > 0 { // The flow ends with a split handler
		for _, v := range lastHandler.continuingFlows {
			nextFlowPipes = append(nextFlowPipes, v.buildCouplings(logger, flowMap, handlerMap, inProg)[0].inputPipe)
		}
	} else if fb.continuingFlow != nil && len(lastHandler.continuingFlows) == 0 { // The flow ends with another flow and a split handler - shouldn't be possible
		err := fmt.Sprintf("Flow %v cannot be completed by another flow and end with a split handler", fb.name)
		panic(err)
	}

	// Associate input pipes as the previous handler's output
	for i, v := range couplings {
		v.outputPipes = make(map[string]*pipe)
		if i == len(fb.handlers)-1 { // Final coupling
			// If this flow is followed by other flows, set name of the following flows and use their input pipes as the output pipe for this handler
			for _, w := range nextFlowPipes {
				v.outputPipes[w.flowName] = w
				v.inputPipe.nextFlows = append(v.inputPipe.nextFlows, w.flowName)
			}
		} else {
			nextInput := couplings[i+1].inputPipe
			v.outputPipes[nextInput.flowName] = nextInput
			v.inputPipe.nextFlows = append(v.inputPipe.nextFlows, nextInput.flowName)
		}
	}

	(*flowMap)[fb.name] = couplings
	inProg.Remove(fb.name)
	return couplings
}

type completedHandlerBuilder struct {
	name            string
	batchPolicy     *batchPolicy
	maxAttempts     *int
	retryPeriod     *time.Duration
	errorFlow       *flowBuilder
	continuingFlows []*flowBuilder
	build           func() *handler
}

func (h *completedHandlerBuilder) applyDefaults(tracker *defaultValueTracker) {
	if tracker.handlers.Contains(h.name) {
		return
	}
	tracker.handlers.Add(h.name)

	if h.errorFlow != nil {
		h.errorFlow.applyDefaults(tracker)
	}

	for _, v := range h.continuingFlows {
		v.applyDefaults(tracker)
	}

	if h.batchPolicy == nil {
		h.batchPolicy = newBatchPolicy(1, time.Second)
	}

	if h.maxAttempts == nil {
		v := 0 // Default: Retry forever
		h.maxAttempts = &v
	}

	if h.retryPeriod == nil {
		v := time.Second
		h.retryPeriod = &v
	}
}

type CompletedHandlerBuilder[Tin, Tout any] completedHandlerBuilder

type HandlerBuilder[Tin, Tout any] struct {
	name        string
	maxAttempts *int
	retryPeriod *time.Duration
	errorFlow   *flowBuilder
}

func (h *HandlerBuilder[Tin, Tout]) WithBatching(batchSize int, period time.Duration) *BatchHandlerBuilder[Tin, Tout] {
	return &BatchHandlerBuilder[Tin, Tout]{
		name:        h.name,
		maxAttempts: h.maxAttempts,
		retryPeriod: h.retryPeriod,
		errorFlow:   h.errorFlow,
		batchSize:   batchSize,
		batchPeriod: period,
	}
}

func (h *HandlerBuilder[Tin, Tout]) WithMaxAttempts(maxAttempts int) *HandlerBuilder[Tin, Tout] {
	v := maxAttempts
	h.maxAttempts = &v
	return h
}

func (h *HandlerBuilder[Tin, Tout]) WithRetryPeriod(period time.Duration) *HandlerBuilder[Tin, Tout] {
	v := period
	h.retryPeriod = &v
	return h
}

func (h *HandlerBuilder[Tin, Tout]) WithErrorHandler(errHandler *CompletedHandlerBuilder[Tin, NilType]) *HandlerBuilder[Tin, Tout] {
	efb := Flow[Tin](h.name + "_Error").
		WithHandler(
			errHandler,
		)
	h.errorFlow = (*flowBuilder)(efb)
	return h
}

func (h *HandlerBuilder[Tin, Tout]) WithErrorFlow(errFlow *FlowBuilder[Tin, NilType]) *HandlerBuilder[Tin, Tout] {
	h.errorFlow = (*flowBuilder)(errFlow)
	return h
}

func (h *HandlerBuilder[Tin, Tout]) WithHandleFunc(f func(context.Context, HandlerContext[Tin, Tout], *Message[Tin]) *HandlerResult[Tout]) *CompletedHandlerBuilder[Tin, Tout] {
	return &CompletedHandlerBuilder[Tin, Tout]{
		name:            h.name,
		batchPolicy:     nil,
		maxAttempts:     h.maxAttempts,
		retryPeriod:     h.retryPeriod,
		errorFlow:       h.errorFlow,
		continuingFlows: make([]*flowBuilder, 0),
		build: func() *handler {
			return newHandler(h.name, f)
		},
	}
}

type BatchHandlerBuilder[Tin, Tout any] struct {
	name        string
	batchSize   int
	batchPeriod time.Duration
	maxAttempts *int
	retryPeriod *time.Duration
	errorFlow   *flowBuilder
}

func (h *BatchHandlerBuilder[Tin, Tout]) WithMaxAttempts(maxAttempts int) *BatchHandlerBuilder[Tin, Tout] {
	v := maxAttempts
	h.maxAttempts = &v
	return h
}

func (h *BatchHandlerBuilder[Tin, Tout]) WithRetryPeriod(period time.Duration) *BatchHandlerBuilder[Tin, Tout] {
	v := period
	h.retryPeriod = &v
	return h
}

func (h *BatchHandlerBuilder[Tin, Tout]) WithErrorHandler(errHandler *CompletedHandlerBuilder[Tin, NilType]) *BatchHandlerBuilder[Tin, Tout] {
	efb := Flow[Tin](h.name + "_Error").
		WithHandler(
			errHandler,
		)
	h.errorFlow = (*flowBuilder)(efb)
	return h
}

func (h *BatchHandlerBuilder[Tin, Tout]) WithErrorFlow(errFlow *FlowBuilder[Tin, NilType]) *BatchHandlerBuilder[Tin, Tout] {
	h.errorFlow = (*flowBuilder)(errFlow)
	return h
}

func (h *BatchHandlerBuilder[Tin, Tout]) WithHandleFunc(f func(context.Context, HandlerContext[Tin, Tout], []*Message[Tin]) []*HandlerResult[Tout]) *CompletedHandlerBuilder[Tin, Tout] {
	return &CompletedHandlerBuilder[Tin, Tout]{
		name:            h.name,
		batchPolicy:     newBatchPolicy(h.batchSize, h.batchPeriod),
		maxAttempts:     h.maxAttempts,
		retryPeriod:     h.retryPeriod,
		errorFlow:       h.errorFlow,
		continuingFlows: make([]*flowBuilder, 0),
		build: func() *handler {
			return newBatchHandler(h.name, f)
		},
	}
}

type EndHandlerBuilder[Tin any] struct {
	name        string
	maxAttempts *int
	retryPeriod *time.Duration
	errorFlow   *flowBuilder
}

func (h *EndHandlerBuilder[Tin]) WithBatching(batchSize int, period time.Duration) *BatchEndHandlerBuilder[Tin] {
	return &BatchEndHandlerBuilder[Tin]{
		name:        h.name,
		maxAttempts: h.maxAttempts,
		retryPeriod: h.retryPeriod,
		errorFlow:   h.errorFlow,
		batchSize:   batchSize,
		batchPeriod: period,
	}
}

func (h *EndHandlerBuilder[Tin]) WithMaxAttempts(maxAttempts int) *EndHandlerBuilder[Tin] {
	v := maxAttempts
	h.maxAttempts = &v
	return h
}

func (h *EndHandlerBuilder[Tin]) WithRetryPeriod(period time.Duration) *EndHandlerBuilder[Tin] {
	v := period
	h.retryPeriod = &v
	return h
}

func (h *EndHandlerBuilder[Tin]) WithErrorHandler(errHandler *CompletedHandlerBuilder[Tin, NilType]) *EndHandlerBuilder[Tin] {
	efb := Flow[Tin](h.name + "_Error").
		WithHandler(
			errHandler,
		)
	h.errorFlow = (*flowBuilder)(efb)
	return h
}

func (h *EndHandlerBuilder[Tin]) WithErrorFlow(errFlow *FlowBuilder[Tin, NilType]) *EndHandlerBuilder[Tin] {
	h.errorFlow = (*flowBuilder)(errFlow)
	return h
}

func (h *EndHandlerBuilder[Tin]) WithHandleFunc(f func(context.Context, EndHandlerContext[Tin], *Message[Tin]) *EndHandlerResult) *CompletedHandlerBuilder[Tin, NilType] {
	return &CompletedHandlerBuilder[Tin, NilType]{
		name:            h.name,
		batchPolicy:     nil,
		maxAttempts:     h.maxAttempts,
		retryPeriod:     h.retryPeriod,
		errorFlow:       h.errorFlow,
		continuingFlows: make([]*flowBuilder, 0),
		build: func() *handler {
			return newEndHandler(h.name, f)
		},
	}
}

type BatchEndHandlerBuilder[Tin any] struct {
	name        string
	batchSize   int
	batchPeriod time.Duration
	maxAttempts *int
	retryPeriod *time.Duration
	errorFlow   *flowBuilder
}

func (h *BatchEndHandlerBuilder[Tin]) WithMaxAttempts(maxAttempts int) *BatchEndHandlerBuilder[Tin] {
	v := maxAttempts
	h.maxAttempts = &v
	return h
}

func (h *BatchEndHandlerBuilder[Tin]) WithRetryPeriod(period time.Duration) *BatchEndHandlerBuilder[Tin] {
	v := period
	h.retryPeriod = &v
	return h
}

func (h *BatchEndHandlerBuilder[Tin]) WithErrorHandler(errHandler *CompletedHandlerBuilder[Tin, NilType]) *BatchEndHandlerBuilder[Tin] {
	efb := Flow[Tin](h.name + "_Error").
		WithHandler(
			errHandler,
		)
	h.errorFlow = (*flowBuilder)(efb)
	return h
}

func (h *BatchEndHandlerBuilder[Tin]) WithErrorFlow(errFlow *FlowBuilder[Tin, NilType]) *BatchEndHandlerBuilder[Tin] {
	h.errorFlow = (*flowBuilder)(errFlow)
	return h
}

func (h *BatchEndHandlerBuilder[Tin]) WithHandleFunc(f func(context.Context, EndHandlerContext[Tin], []*Message[Tin]) []*EndHandlerResult) *CompletedHandlerBuilder[Tin, NilType] {
	return &CompletedHandlerBuilder[Tin, NilType]{
		name:            h.name,
		batchPolicy:     newBatchPolicy(h.batchSize, h.batchPeriod),
		maxAttempts:     h.maxAttempts,
		retryPeriod:     h.retryPeriod,
		errorFlow:       h.errorFlow,
		continuingFlows: make([]*flowBuilder, 0),
		build: func() *handler {
			return newBatchEndHandler(h.name, f)
		},
	}
}

type SplitHandlerBuilder[Tin any] struct {
	name            string
	maxAttempts     *int
	retryPeriod     *time.Duration
	errorFlow       *flowBuilder
	continuingFlows []*flowBuilder
}

func (h *SplitHandlerBuilder[Tin]) WithBatching(batchSize int, period time.Duration) *BatchSplitHandlerBuilder[Tin] {
	return &BatchSplitHandlerBuilder[Tin]{
		name:            h.name,
		maxAttempts:     h.maxAttempts,
		retryPeriod:     h.retryPeriod,
		errorFlow:       h.errorFlow,
		continuingFlows: h.continuingFlows,
		batchSize:       batchSize,
		batchPeriod:     period,
	}
}

func (h *SplitHandlerBuilder[Tin]) WithMaxAttempts(maxAttempts int) *SplitHandlerBuilder[Tin] {
	v := maxAttempts
	h.maxAttempts = &v
	return h
}

func (h *SplitHandlerBuilder[Tin]) WithRetryPeriod(period time.Duration) *SplitHandlerBuilder[Tin] {
	v := period
	h.retryPeriod = &v
	return h
}

func (h *SplitHandlerBuilder[Tin]) WithErrorHandler(errHandler *CompletedHandlerBuilder[Tin, NilType]) *SplitHandlerBuilder[Tin] {
	efb := Flow[Tin](h.name + "_Error").
		WithHandler(
			errHandler,
		)
	h.errorFlow = (*flowBuilder)(efb)
	return h
}

func (h *SplitHandlerBuilder[Tin]) WithErrorFlow(errFlow *FlowBuilder[Tin, NilType]) *SplitHandlerBuilder[Tin] {
	h.errorFlow = (*flowBuilder)(errFlow)
	return h
}

func (h *SplitHandlerBuilder[Tin]) WithFlow(flow *FlowBuilder[Tin, NilType]) *SplitHandlerBuilder[Tin] {
	h.continuingFlows = append(h.continuingFlows, (*flowBuilder)(flow))
	return h
}

func (h *SplitHandlerBuilder[Tin]) WithHandleFunc(f func(context.Context, SplitHandlerContext[Tin], *Message[Tin]) *SplitHandlerResult) *CompletedHandlerBuilder[Tin, NilType] {
	return &CompletedHandlerBuilder[Tin, NilType]{
		name:            h.name,
		batchPolicy:     nil,
		maxAttempts:     h.maxAttempts,
		retryPeriod:     h.retryPeriod,
		errorFlow:       h.errorFlow,
		continuingFlows: h.continuingFlows,
		build: func() *handler {
			return newSplitHandler(h.name, f)
		},
	}
}

type BatchSplitHandlerBuilder[Tin any] struct {
	name            string
	batchSize       int
	batchPeriod     time.Duration
	maxAttempts     *int
	retryPeriod     *time.Duration
	errorFlow       *flowBuilder
	continuingFlows []*flowBuilder
}

func (h *BatchSplitHandlerBuilder[Tin]) WithMaxAttempts(maxAttempts int) *BatchSplitHandlerBuilder[Tin] {
	v := maxAttempts
	h.maxAttempts = &v
	return h
}

func (h *BatchSplitHandlerBuilder[Tin]) WithRetryPeriod(period time.Duration) *BatchSplitHandlerBuilder[Tin] {
	v := period
	h.retryPeriod = &v
	return h
}

func (h *BatchSplitHandlerBuilder[Tin]) WithErrorHandler(errHandler *CompletedHandlerBuilder[Tin, NilType]) *BatchSplitHandlerBuilder[Tin] {
	efb := Flow[Tin](h.name + "_Error").
		WithHandler(
			errHandler,
		)
	h.errorFlow = (*flowBuilder)(efb)
	return h
}

func (h *BatchSplitHandlerBuilder[Tin]) WithErrorFlow(errFlow *FlowBuilder[Tin, NilType]) *BatchSplitHandlerBuilder[Tin] {
	h.errorFlow = (*flowBuilder)(errFlow)
	return h
}

func (h *BatchSplitHandlerBuilder[Tin]) WithFlow(flow *FlowBuilder[Tin, NilType]) *BatchSplitHandlerBuilder[Tin] {
	h.continuingFlows = append(h.continuingFlows, (*flowBuilder)(flow))
	return h
}

func (h *BatchSplitHandlerBuilder[Tin]) WithHandleFunc(f func(context.Context, SplitHandlerContext[Tin], []*Message[Tin]) []*SplitHandlerResult) *CompletedHandlerBuilder[Tin, NilType] {
	return &CompletedHandlerBuilder[Tin, NilType]{
		name:            h.name,
		batchPolicy:     newBatchPolicy(h.batchSize, h.batchPeriod),
		maxAttempts:     h.maxAttempts,
		retryPeriod:     h.retryPeriod,
		errorFlow:       h.errorFlow,
		continuingFlows: h.continuingFlows,
		build: func() *handler {
			return newBatchSplitHandler(h.name, f)
		},
	}
}

type ComponentBuilder struct {
	name           string
	handlers       []string
	maxConcurrency *int
}

func (c *ComponentBuilder) WithHandlers(handlerNames ...string) *ComponentBuilder {
	c.handlers = handlerNames
	return c
}

func (c *ComponentBuilder) WithConcurrency(maxConcurrency int) *ComponentBuilder {
	v := maxConcurrency
	c.maxConcurrency = &v
	return c
}

func (c *ComponentBuilder) applyDefaults(tracker *defaultValueTracker) {
	if tracker.components.Contains(c.name) {
		return
	}
	tracker.components.Add(c.name)

	if c.maxConcurrency == nil {
		v := 10
		c.maxConcurrency = &v
	}
}

func (c *ComponentBuilder) validate(handlerNames *util.Set[string]) *validationResult {
	r := newValidationResult()

	if len(c.handlers) == 0 {
		r.addError("Component %v must contain at least 1 handler", c.name)
	}

	for _, v := range c.handlers {
		if ok := handlerNames.Contains(v); !ok {
			r.addError("Component %v references non-existent handler %v", c.name, v)
		}
	}

	return r
}

type BuiltApp struct {
	name       string
	flows      map[string]*flow
	components []*component
	ctx        context.Context // Lifetime context
	cancel     context.CancelFunc
}

func (a *BuiltApp) GetFlow[T any](name string) (*BuiltFlow[T], error) {
	if f, ok := a.flows[name]; !ok {
		return nil, errors.New("Flow doesn't exist")
	} else if f.triggerT != reflect.TypeFor[T]() {
		return nil, errors.New("Flow doesn't have requested type")
	} else {
		return &BuiltFlow[T]{
			flow: f,
		}, nil
	}
}

func (a *BuiltApp) Stop(ctx context.Context) bool {
	// Close all the input pipes to prevent the flows being triggered any more
	stopChans := make([]*<-chan struct{}, 0, len(a.flows))
	for _, v := range a.flows {
		close(v.inputPipe.input)
		var c <-chan struct{} = v.endComponent.stopped

		// Multiple flows might finish with the same component, only add it to the slice if it hasn't been added already
		if !slices.Contains(stopChans, &c) {
			stopChans = append(stopChans, &c)
		}
	}

	// Wait for either all the flows to stop, or for the provided context to be cancelled
	var x struct{}
	var ok bool
	for len(stopChans) > 0 {
		if i, err := arraySelect(ctx, stopChans, &x, &ok); err != nil {
			// Context has been cancelled, tell the app to force shutdown
			a.cancel()
			return false
		} else {
			// Remove the stopped channel
			stopChans = append(stopChans[:i], stopChans[i+1:]...)
		}
	}

	// Return true if the app stopped gracefully
	return true
}

type flow struct {
	triggerT     reflect.Type
	name         string
	inputPipe    *pipe
	endComponent *component // The component that ends the flow
}

func (f *flow) trigger(ctx context.Context, data ...any) error {
	return f.inputPipe.send(ctx, data...)
}

type BuiltFlow[T any] struct {
	flow *flow
}

func (f *BuiltFlow[T]) Trigger(ctx context.Context, data ...T) error {
	conv := make([]any, len(data))
	for i, v := range data {
		conv[i] = v
	}
	return f.flow.trigger(ctx, conv...)
}
