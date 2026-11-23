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

// TODO - all the comments
// TODO - check that validation catches a single handler in multiple components
// TODO - return error from trigger if app is stopping

// App returns an AppBuilder instance with the provided name.
// Call the methods on the AppBuilder to define its flows and components, followed by Build() to build and start the app.
func App(name string) *AppBuilder {
	return &AppBuilder{
		name:  name,
		flows: make([]*flowBuilder, 0),
	}
}

// Flow returns a FlowBuilder instance for defining a flow with the provided name.
// Tinit is the type of data that triggers the flow (by sending it to the first handler).
// It exposes methods for adding handlers to the flow, or triggering a different flow from this one.
func Flow[Tinit any](name string) *FlowBuilder[Tinit, Tinit] {
	return &FlowBuilder[Tinit, Tinit]{
		name:     name,
		triggerT: reflect.TypeFor[Tinit](),
		handlers: make([]*completedHandlerBuilder, 0),
	}
}

// Handler returns a HandlerBuilder instance for defining a handler which receives messages,
// and then optionally outputs messages for a single following handler.
// Tin is the type of data which is received by the handler, and Tout is the type of data which it returns for the next handler.
// The name provided for the handler must be unique across all handlers in the app.
func Handler[Tin, Tout any](name string) *HandlerBuilder[Tin, Tout] {
	return &HandlerBuilder[Tin, Tout]{
		name:        name,
		maxAttempts: nil,
		retryPeriod: nil,
	}
}

// EndHandler returns an EndHandlerBuilder instance for defining a handler which receives messages at the end of a flow.
// Tin is the type of data which is received by the handler.
// The name provided for the handler must be unique across all handlers in the app.
func EndHandler[Tin any](name string) *EndHandlerBuilder[Tin] {
	return &EndHandlerBuilder[Tin]{
		name: name,
	}
}

// SplitHandler returns a SplitHandlerBuilder instance for defining a handler which receives messages,
// and then optionally outputs messages to multiple following handlers.
// Tin is the type of data which is received by the handler.
// The name provided for the handler must be unique across all handlers in the app.
func SplitHandler[Tin any](name string) *SplitHandlerBuilder[Tin] {
	return &SplitHandlerBuilder[Tin]{
		name: name,
	}
}

// Component returns a ComponentBuilder instance for defining a component which groups together
// handlers in a component.
// Handlers grouped in a component share a common concurrency limit.
// The name provided for the component must be unique across all components in the app.
func Component(name string) *ComponentBuilder {
	return &ComponentBuilder{
		name: name,
	}
}

type NilType struct{}

// AppBuilder exposes methods for adding flow and component definitions to the app, as well as
// defining common app-level configuration like the logger instance to use.
type AppBuilder struct {
	name       string
	flows      []*flowBuilder
	components []*ComponentBuilder
	logger     *slog.Logger
}

// WithFlow adds the flow with the provided flow definition to the app.
// Tinit is the type of data received by the first handler in the provided flow.
// Create a FlowBuilder with Flow[Tinit]("name").
// Returns the AppBuilder for chaining further calls.
func (a *AppBuilder) WithFlow[Tinit any](flow *FlowBuilder[Tinit, NilType]) *AppBuilder {
	a.flows = append(a.flows, (*flowBuilder)(flow))
	return a
}

// WithComponents configures the components for the app, using the provided component builders.
// Create a ComponentBuilder with Component("name").
// Returns the AppBuilder for chaining further calls.
func (a *AppBuilder) WithComponents(componentBuilders ...*ComponentBuilder) *AppBuilder {
	a.components = componentBuilders
	return a
}

// WithLogger configures the logger the app will use.
// Returns the AppBuilder for chaining further calls.
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

// Build builds the app using the configured flows, handlers and components, and starts all processing loops.
// If the AppBuilder instance is configured correctly, it will return a BuiltApp instance for the app.
// If there are issues with the configuration, it returns an aggregate error with all found validation failures.
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

// FlowBuilder exposes methods for building a flow out of handlers, or using the flow to trigger
// another flow. Tinit is the type of data that triggers the first handler in the flow, while Tnext
// is the type of data that's sent to the next handler while building out the handler chain.
// If the type of Tnext is NilType, then the flow is complete.
type FlowBuilder[Tinit, Tnext any] flowBuilder

// WithHandler adds a handler (of any kind) to the flow, as configured by the provided handler builder.
// Returns the flow builder for chaining further calls.
func (fb *FlowBuilder[Tinit, Tnext]) WithHandler[Tout any](hb *CompletedHandlerBuilder[Tnext, Tout]) *FlowBuilder[Tinit, Tout] {
	fb.handlers = append(fb.handlers, (*completedHandlerBuilder)(hb))
	return fb.toNextBuilder[Tout]()
}

// WithFlow triggers another flow from this one, completing this flow. The trigger type for the
// triggered flow must be the same as the next data type for this flow.
// Returns the flow builder for chaining further configuration calls.
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

// CompletedHandlerBuilder represents a handler builder that has had its handler function defined,
// and is ready for passing to a FlowBuilder instance as part of a call to WithHandler.
type CompletedHandlerBuilder[Tin, Tout any] completedHandlerBuilder

// HandlerBuilder represents an in-progress build of a handler definition, which accepts single
// messages of one type, and returns messages of another type.
// Tin is the data type sent to the handler, while Tout is the data type returned by the handler.
type HandlerBuilder[Tin, Tout any] struct {
	name        string
	maxAttempts *int
	retryPeriod *time.Duration
	errorFlow   *flowBuilder
}

// WithBatching configures the handler to receive and return batches of messages instead of single messages.
// batchSize is the maximum permitted size of batches, while period is the maximum duration a batch
// should be pending before being sent to the handler.
// Returns a BatchHandlerBuilder in order to continue configuration of the handler.
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

// WithMaxAttempts configures the handler to abandon processing of a message once it has failed to be
// processed maxAttempts times.
// By default, a handler will retry indefinitely with a 1 second delay between each attempt.
// When handling is abandoned, an error handler or flow can be triggered for the message using WithErrorHandler or WithErrorFlow.
// Returns the HandlerBuilder for chaining further calls.
func (h *HandlerBuilder[Tin, Tout]) WithMaxAttempts(maxAttempts int) *HandlerBuilder[Tin, Tout] {
	v := maxAttempts
	h.maxAttempts = &v
	return h
}

// WithRetryPeriod sets the time period that should elapse between attempts to process a message.
// Defaults to 1 second.
// Returns the HandlerBuilder for chaining further calls.
func (h *HandlerBuilder[Tin, Tout]) WithRetryPeriod(period time.Duration) *HandlerBuilder[Tin, Tout] {
	v := period
	h.retryPeriod = &v
	return h
}

// WithErrorHandler specifies a handler to which any message is sent if it exceeds its maximum
// permitted number of attempts for this handler. Callers should call either this or WithErrorFlow,
// not both.
// By default, no error handler is provided and messages will be logged then dropped.
// Returns the HandlerBuilder for chaining further calls.
func (h *HandlerBuilder[Tin, Tout]) WithErrorHandler(errHandler *CompletedHandlerBuilder[Tin, NilType]) *HandlerBuilder[Tin, Tout] {
	efb := Flow[Tin](h.name + "_Error").
		WithHandler(
			errHandler,
		)
	h.errorFlow = (*flowBuilder)(efb)
	return h
}

// WithErrorFlow specifies a flow that is triggered for any message that exceeds its maximum
// permitted number of attempts in this handler. Callers should call either this or WithErrorHandler,
// not both.
// By default, no flow is provided and messages will be logged then dropped.
// Returns the HandlerBuilder for chaining further calls.
func (h *HandlerBuilder[Tin, Tout]) WithErrorFlow(errFlow *FlowBuilder[Tin, NilType]) *HandlerBuilder[Tin, Tout] {
	h.errorFlow = (*flowBuilder)(errFlow)
	return h
}

// WithHandleFunc specifies the function that will be invoked when this handler handles a message.
// Calling this method completes the handler specification, and no further function calls can be
// made on this builder.
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

// BatchHandlerBuilder represents an in-progress build of a batch handler definition, which accepts
// batches of messages of one type, and returns batches of messages of another type.
// Tin is the data type sent to the handler, while Tout is the data type returned by the handler.
type BatchHandlerBuilder[Tin, Tout any] struct {
	name        string
	batchSize   int
	batchPeriod time.Duration
	maxAttempts *int
	retryPeriod *time.Duration
	errorFlow   *flowBuilder
}

// WithMaxAttempts configures the handler to abandon processing of a message once it has failed to be
// processed maxAttempts times.
// By default, a handler will retry indefinitely with a 1 second delay between each attempt.
// When handling is abandoned, an error handler or flow can be triggered for the message using WithErrorHandler or WithErrorFlow.
// Returns the BatchHandlerBuilder for chaining further calls.
func (h *BatchHandlerBuilder[Tin, Tout]) WithMaxAttempts(maxAttempts int) *BatchHandlerBuilder[Tin, Tout] {
	v := maxAttempts
	h.maxAttempts = &v
	return h
}

// WithRetryPeriod sets the time period that should elapse between attempts to process a message.
// Defaults to 1 second.
// Returns the BatchHandlerBuilder for chaining further calls.
func (h *BatchHandlerBuilder[Tin, Tout]) WithRetryPeriod(period time.Duration) *BatchHandlerBuilder[Tin, Tout] {
	v := period
	h.retryPeriod = &v
	return h
}

// WithErrorHandler specifies a handler to which any message is sent if it exceeds its maximum
// permitted number of attempts for this handler. Callers should call either this or WithErrorFlow,
// not both.
// By default, no error handler is provided and messages will be logged then dropped.
// Returns the BatchHandlerBuilder for chaining further calls.
func (h *BatchHandlerBuilder[Tin, Tout]) WithErrorHandler(errHandler *CompletedHandlerBuilder[Tin, NilType]) *BatchHandlerBuilder[Tin, Tout] {
	efb := Flow[Tin](h.name + "_Error").
		WithHandler(
			errHandler,
		)
	h.errorFlow = (*flowBuilder)(efb)
	return h
}

// WithErrorFlow specifies a flow that is triggered for any message that exceeds its maximum
// permitted number of attempts in this handler. Callers should call either this or WithErrorHandler,
// not both.
// By default, no flow is provided and messages will be logged then dropped.
// Returns the BatchHandlerBuilder for chaining further calls.
func (h *BatchHandlerBuilder[Tin, Tout]) WithErrorFlow(errFlow *FlowBuilder[Tin, NilType]) *BatchHandlerBuilder[Tin, Tout] {
	h.errorFlow = (*flowBuilder)(errFlow)
	return h
}

// WithHandleFunc specifies the function that will be invoked when this handler handles a batch of
// messages.
// Calling this method completes the handler specification, and no further function calls can be
// made on this builder.
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

// EndHandlerBuilder represents an in-progress build of an end handler definition, which accepts
// a single message of one type, and does not return any messages for further handlers.
// Tin is the data type sent to the handler.
type EndHandlerBuilder[Tin any] struct {
	name        string
	maxAttempts *int
	retryPeriod *time.Duration
	errorFlow   *flowBuilder
}

// WithBatching configures the handler to receive batches of messages instead of single messages.
// batchSize is the maximum permitted size of batches, while period is the maximum duration a batch
// should be pending before being sent to the handler.
// Returns a BatchEndHandlerBuilder in order to continue configuration of the handler.
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

// WithMaxAttempts configures the handler to abandon processing of a message once it has failed to be
// processed maxAttempts times.
// By default, a handler will retry indefinitely with a 1 second delay between each attempt.
// When handling is abandoned, an error handler or flow can be triggered for the message using WithErrorHandler or WithErrorFlow.
// Returns the EndHandlerBuilder for chaining further calls.
func (h *EndHandlerBuilder[Tin]) WithMaxAttempts(maxAttempts int) *EndHandlerBuilder[Tin] {
	v := maxAttempts
	h.maxAttempts = &v
	return h
}

// WithRetryPeriod sets the time period that should elapse between attempts to process a message.
// Defaults to 1 second.
// Returns the EndHandlerBuilder for chaining further calls.
func (h *EndHandlerBuilder[Tin]) WithRetryPeriod(period time.Duration) *EndHandlerBuilder[Tin] {
	v := period
	h.retryPeriod = &v
	return h
}

// WithErrorHandler specifies a handler to which any message is sent if it exceeds its maximum
// permitted number of attempts for this handler. Callers should call either this or WithErrorFlow,
// not both.
// By default, no error handler is provided and messages will be logged then dropped.
// Returns the EndHandlerBuilder for chaining further calls.
func (h *EndHandlerBuilder[Tin]) WithErrorHandler(errHandler *CompletedHandlerBuilder[Tin, NilType]) *EndHandlerBuilder[Tin] {
	efb := Flow[Tin](h.name + "_Error").
		WithHandler(
			errHandler,
		)
	h.errorFlow = (*flowBuilder)(efb)
	return h
}

// WithErrorFlow specifies a flow that is triggered for any message that exceeds its maximum
// permitted number of attempts in this handler. Callers should call either this or WithErrorHandler,
// not both.
// By default, no flow is provided and messages will be logged then dropped.
// Returns the EndHandlerBuilder for chaining further calls.
func (h *EndHandlerBuilder[Tin]) WithErrorFlow(errFlow *FlowBuilder[Tin, NilType]) *EndHandlerBuilder[Tin] {
	h.errorFlow = (*flowBuilder)(errFlow)
	return h
}

// WithHandleFunc specifies the function that will be invoked when this handler handles a message.
// Calling this method completes the handler specification, and no further function calls can be
// made on this builder.
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

// BatchEndHandlerBuilder represents an in-progress build of an batched end handler definition, which accepts
// batches of messages of one type, and does not return any messages for further handlers.
// Tin is the data type sent to the handler.
type BatchEndHandlerBuilder[Tin any] struct {
	name        string
	batchSize   int
	batchPeriod time.Duration
	maxAttempts *int
	retryPeriod *time.Duration
	errorFlow   *flowBuilder
}

// WithMaxAttempts configures the handler to abandon processing of a message once it has failed to be
// processed maxAttempts times.
// By default, a handler will retry indefinitely with a 1 second delay between each attempt.
// When handling is abandoned, an error handler or flow can be triggered for the message using WithErrorHandler or WithErrorFlow.
// Returns the BatchEndHandlerBuilder for chaining further calls.
func (h *BatchEndHandlerBuilder[Tin]) WithMaxAttempts(maxAttempts int) *BatchEndHandlerBuilder[Tin] {
	v := maxAttempts
	h.maxAttempts = &v
	return h
}

// WithRetryPeriod sets the time period that should elapse between attempts to process a message.
// Defaults to 1 second.
// Returns the BatchEndHandlerBuilder for chaining further calls.
func (h *BatchEndHandlerBuilder[Tin]) WithRetryPeriod(period time.Duration) *BatchEndHandlerBuilder[Tin] {
	v := period
	h.retryPeriod = &v
	return h
}

// WithErrorHandler specifies a handler to which any message is sent if it exceeds its maximum
// permitted number of attempts for this handler. Callers should call either this or WithErrorFlow,
// not both.
// By default, no error handler is provided and messages will be logged then dropped.
// Returns the BatchEndHandlerBuilder for chaining further calls.
func (h *BatchEndHandlerBuilder[Tin]) WithErrorHandler(errHandler *CompletedHandlerBuilder[Tin, NilType]) *BatchEndHandlerBuilder[Tin] {
	efb := Flow[Tin](h.name + "_Error").
		WithHandler(
			errHandler,
		)
	h.errorFlow = (*flowBuilder)(efb)
	return h
}

// WithErrorFlow specifies a flow that is triggered for any message that exceeds its maximum
// permitted number of attempts in this handler. Callers should call either this or WithErrorHandler,
// not both.
// By default, no flow is provided and messages will be logged then dropped.
// Returns the BatchEndHandlerBuilder for chaining further calls.
func (h *BatchEndHandlerBuilder[Tin]) WithErrorFlow(errFlow *FlowBuilder[Tin, NilType]) *BatchEndHandlerBuilder[Tin] {
	h.errorFlow = (*flowBuilder)(errFlow)
	return h
}

// WithHandleFunc specifies the function that will be invoked when this handler handles a message.
// Calling this method completes the handler specification, and no further function calls can be
// made on this builder.
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

// SplitHandlerBuilder represents an in-progress build of an split handler definition, which accepts
// a message of one type, and returns a number of messages of different types for sending to multiple
// following handlers in different flows.
// Split handlers are always the end of one flow and the beginning of at least two more.
// Tin is the data type sent to the handler.
type SplitHandlerBuilder[Tin any] struct {
	name            string
	maxAttempts     *int
	retryPeriod     *time.Duration
	errorFlow       *flowBuilder
	continuingFlows []*flowBuilder
}

// WithBatching configures the handler to receive batches of messages instead of single messages.
// batchSize is the maximum permitted size of batches, while period is the maximum duration a batch
// should be pending before being sent to the handler.
// Returns a BatchSplitHandlerBuilder in order to continue configuration of the handler.
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

// WithMaxAttempts configures the handler to abandon processing of a message once it has failed to be
// processed maxAttempts times.
// By default, a handler will retry indefinitely with a 1 second delay between each attempt.
// When handling is abandoned, an error handler or flow can be triggered for the message using WithErrorHandler or WithErrorFlow.
// Returns the SplitHandlerBuilder for chaining further calls.
func (h *SplitHandlerBuilder[Tin]) WithMaxAttempts(maxAttempts int) *SplitHandlerBuilder[Tin] {
	v := maxAttempts
	h.maxAttempts = &v
	return h
}

// WithRetryPeriod sets the time period that should elapse between attempts to process a message.
// Defaults to 1 second.
// Returns the SplitHandlerBuilder for chaining further calls.
func (h *SplitHandlerBuilder[Tin]) WithRetryPeriod(period time.Duration) *SplitHandlerBuilder[Tin] {
	v := period
	h.retryPeriod = &v
	return h
}

// WithErrorHandler specifies a handler to which any message is sent if it exceeds its maximum
// permitted number of attempts for this handler. Callers should call either this or WithErrorFlow,
// not both.
// By default, no error handler is provided and messages will be logged then dropped.
// Returns the SplitHandlerBuilder for chaining further calls.
func (h *SplitHandlerBuilder[Tin]) WithErrorHandler(errHandler *CompletedHandlerBuilder[Tin, NilType]) *SplitHandlerBuilder[Tin] {
	efb := Flow[Tin](h.name + "_Error").
		WithHandler(
			errHandler,
		)
	h.errorFlow = (*flowBuilder)(efb)
	return h
}

// WithErrorFlow specifies a flow that is triggered for any message that exceeds its maximum
// permitted number of attempts in this handler. Callers should call either this or WithErrorHandler,
// not both.
// By default, no flow is provided and messages will be logged then dropped.
// Returns the SplitHandlerBuilder for chaining further calls.
func (h *SplitHandlerBuilder[Tin]) WithErrorFlow(errFlow *FlowBuilder[Tin, NilType]) *SplitHandlerBuilder[Tin] {
	h.errorFlow = (*flowBuilder)(errFlow)
	return h
}

// WithFlow adds a possible flow that can be triggered by this split handler. Within the handle
// function, possible flows that can be triggered by that handler are referred to by their name.
// Client code is obligated to return messages of the correct type for any flows triggered by the
// handler.
// Returns the SplitHandlerBuilder for chaining further calls.
func (h *SplitHandlerBuilder[Tin]) WithFlow(flow *FlowBuilder[Tin, NilType]) *SplitHandlerBuilder[Tin] {
	h.continuingFlows = append(h.continuingFlows, (*flowBuilder)(flow))
	return h
}

// WithHandleFunc specifies the function that will be invoked when this handler handles a message.
// Calling this method completes the handler specification, and no further function calls can be
// made on this builder.
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

// BatchSplitHandlerBuilder represents an in-progress build of a batched split handler definition,
// which accepts a batch of messages of one type, and returns a number of messages of different
// types for sending to multiple following handlers in different flows.
// Split handlers are always the end of one flow and the beginning of at least two more.
// Tin is the data type sent to the handler.
type BatchSplitHandlerBuilder[Tin any] struct {
	name            string
	batchSize       int
	batchPeriod     time.Duration
	maxAttempts     *int
	retryPeriod     *time.Duration
	errorFlow       *flowBuilder
	continuingFlows []*flowBuilder
}

// WithMaxAttempts configures the handler to abandon processing of a message once it has failed to be
// processed maxAttempts times.
// By default, a handler will retry indefinitely with a 1 second delay between each attempt.
// When handling is abandoned, an error handler or flow can be triggered for the message using WithErrorHandler or WithErrorFlow.
// Returns the BatchSplitHandlerBuilder for chaining further calls.
func (h *BatchSplitHandlerBuilder[Tin]) WithMaxAttempts(maxAttempts int) *BatchSplitHandlerBuilder[Tin] {
	v := maxAttempts
	h.maxAttempts = &v
	return h
}

// WithRetryPeriod sets the time period that should elapse between attempts to process a message.
// Defaults to 1 second.
// Returns the BatchSplitHandlerBuilder for chaining further calls.
func (h *BatchSplitHandlerBuilder[Tin]) WithRetryPeriod(period time.Duration) *BatchSplitHandlerBuilder[Tin] {
	v := period
	h.retryPeriod = &v
	return h
}

// WithErrorHandler specifies a handler to which any message is sent if it exceeds its maximum
// permitted number of attempts for this handler. Callers should call either this or WithErrorFlow,
// not both.
// By default, no error handler is provided and messages will be logged then dropped.
// Returns the BatchSplitHandlerBuilder for chaining further calls.
func (h *BatchSplitHandlerBuilder[Tin]) WithErrorHandler(errHandler *CompletedHandlerBuilder[Tin, NilType]) *BatchSplitHandlerBuilder[Tin] {
	efb := Flow[Tin](h.name + "_Error").
		WithHandler(
			errHandler,
		)
	h.errorFlow = (*flowBuilder)(efb)
	return h
}

// WithErrorFlow specifies a flow that is triggered for any message that exceeds its maximum
// permitted number of attempts in this handler. Callers should call either this or WithErrorHandler,
// not both.
// By default, no flow is provided and messages will be logged then dropped.
// Returns the BatchSplitHandlerBuilder for chaining further calls.
func (h *BatchSplitHandlerBuilder[Tin]) WithErrorFlow(errFlow *FlowBuilder[Tin, NilType]) *BatchSplitHandlerBuilder[Tin] {
	h.errorFlow = (*flowBuilder)(errFlow)
	return h
}

// WithFlow adds a possible flow that can be triggered by this split handler. Within the handle
// function, possible flows that can be triggered by that handler are referred to by their name.
// Client code is obligated to return messages of the correct type for any flows triggered by the
// handler.
// Returns the BatchSplitHandlerBuilder for chaining further calls.
func (h *BatchSplitHandlerBuilder[Tin]) WithFlow(flow *FlowBuilder[Tin, NilType]) *BatchSplitHandlerBuilder[Tin] {
	h.continuingFlows = append(h.continuingFlows, (*flowBuilder)(flow))
	return h
}

// WithHandleFunc specifies the function that will be invoked when this handler handles a message.
// Calling this method completes the handler specification, and no further function calls can be
// made on this builder.
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

// ComponentBuilder exposes methods for configuring a component by grouping handlers together with a
// common concurrency pool.
type ComponentBuilder struct {
	name           string
	handlers       []string
	maxConcurrency *int
}

// WithHandlers specifies the handlers which make up this component.
// Handlers can only belong to a single component, and are referred to by name.
// Returns the ComponentBuilder for chaining further calls.
func (c *ComponentBuilder) WithHandlers(handlerNames ...string) *ComponentBuilder {
	c.handlers = handlerNames
	return c
}

// WithConcurrency specifies how many handlers in this component can execute concurrently at any
// given time.
// Defaults to 10.
// Returns the ComponentBuilder for chaining further calls.
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

// BuiltApp is an app that has been built from an AppBuilder by calling Build().
// All processing loops are active, and will continue to run until Stop() has been called.
type BuiltApp struct {
	name       string
	flows      map[string]*flow
	components []*component
	ctx        context.Context // Lifetime context
	cancel     context.CancelFunc
}

// GetFlow returns the flow with the specified name, for triggering with messages.
// T is the type of data that triggers the flow, and must match the data type provided when
// building the flow.
// Returns the built flow, or an error if there is no flow with the provided name, or T does not
// match the data type that was provided when the flow was configured.
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

// Stop stops the app and shuts down all processing loops. Once called, any attempts to trigger
// flows will return an error. Any flows that are currently in progress will finish processing,
// and components will shut down once no more work available.
// This call blocks until the app has stopped successfully. If the provided context is cancelled,
// the app is force stopped, and any in progress flows are forcefully terminated, dropping any
// in progress messages.
// Returns true if the app stopped gracefully, or false if the app was forcefully stopped.
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

// BuiltFlow represents a single flow that form part of a running BuiltApp.
// It exposes a method for triggering the flow with new data.
type BuiltFlow[T any] struct {
	flow *flow
}

// Triggers the flow by sending the provided data to the first handler in the flow.
// Returns an error if the provided context is cancelled while waiting for the data
// to be handed over to the flow pipeline, or if the app is currently being stopped.
// This call is non-blocking - it returns as soon as the data has been successfully
// handed over to the flow pipeline.
func (f *BuiltFlow[T]) Trigger(ctx context.Context, data ...T) error {
	conv := make([]any, len(data))
	for i, v := range data {
		conv[i] = v
	}
	return f.flow.trigger(ctx, conv...)
}
