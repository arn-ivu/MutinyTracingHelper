package de.ivu.arn.helper;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.quarkus.opentelemetry.runtime.QuarkusContextStorage;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;

//this functionality will hopefully be provided by the quarkus-opentelemetry extension in the future
//see https://github.com/quarkusio/quarkus/issues/44411
public class MutinyTracingHelper {

    private static final String SPAN_STACK = "SPAN_STACK";

    private static final Tracer TRACER = GlobalOpenTelemetry.getTracer("de.ivu.arn");


    /**
     * Wraps the given pipeline with a span with the given name. Ensures that subspans find the current span as context,
     * by running on a duplicated context. The span will be closed when the pipeline completes.
     * If there is already a span in the current context, it will be used as parent for the new span.
     * <p>
     * Use as follows:
     * Given this existing pipeline:
     * ```java
     * Uni.createFrom().item("Hello")
     * .onItem().transform(s -> s + " World")
     * .subscribe().with(System.out::println);
     * ```
     * wrap like this:
     * ```java
     * Uni.createFrom().item("Hello")
     * .onItem().transformToUni(s -> wrapWithSpan("mySpan", Uni.createFrom().item(s + " World")))
     * .subscribe().with(System.out::println);
     * ```
     * <p>
     * it also works with multi:
     * ```java
     * Multi.createFrom().items("Alice", "Bob", "Charlie")
     * .onItem().transform(name -> "Hello " + name)
     * .subscribe().with(System.out::println);
     * ```
     * wrap like this:
     * ```java
     * Multi.createFrom().items("Alice", "Bob", "Charlie")
     * .onItem().transformToUni(s -> wrapWithSpan("mySpan", Uni.createFrom().item("Hello " + s)
     * .onItem().transform(name -> "Hello " + name)
     * ))
     * .subscribe().with(System.out::println);
     * ```
     *
     * @param <T>
     * @param spanName
     *         the name of the span that should be created
     * @param pipeline
     *         the pipeline to run within the span
     *
     * @return the result of the pipeline
     */
    public static <T> Uni<T> wrapWithSpan(Tracer tracer, final String spanName, final Uni<T> pipeline) {

        return wrapWithSpan(tracer, Optional.of(io.opentelemetry.context.Context.current()), spanName, pipeline);
    }

    /**
     * see {@link #wrapWithSpan(Tracer, String, Uni)}
     * uses the default tracer
     *
     * @param spanName the name of the span that should be created
     * @param pipeline the pipeline to run within the span
     * @return the result of the pipeline
     * @param <T>
     */
    public static <T> Uni<T> wrapWithSpan(final String spanName, final Uni<T> pipeline) {

        return wrapWithSpan(TRACER, Optional.of(io.opentelemetry.context.Context.current()), spanName, pipeline);
    }

    /***
     * see {@link #wrapWithSpan(Tracer, String, Uni)} uses the default tracer
     *
     * @param parentContext
     * @param spanName
     * @param pipeline
     * @return
     * @param <T>
     */
    public static <T> Uni<T> wrapWithSpan(final Optional<io.opentelemetry.context.Context> parentContext,
            final String spanName, final Uni<T> pipeline) {

        return wrapWithSpan(TRACER, parentContext, spanName, pipeline);
    }


    /**
     * see {@link #wrapWithSpan(Tracer, String, Uni)}
     *
     * @param <T>
     * @param parentContext
     *         the parent context to use for the new span. If empty, a new root span will be created.
     * @param spanName
     *         the name of the span that should be created
     * @param pipeline
     *         the pipeline to run within the span
     *
     * @return the result of the pipeline
     */
    public static <T> Uni<T> wrapWithSpan(Tracer tracer, final Optional<io.opentelemetry.context.Context> parentContext,
            final String spanName, final Uni<T> pipeline) {

        //creates duplicate context, if the current context  is not a duplicated one and not null
        //Otherwise returns the current context or null
        final Context context = QuarkusContextStorage.getVertxContext();

        return Uni.createFrom().voidItem()
                .emitOn(runnable -> {
                    if(context != null) {
                        context.runOnContext(v -> runnable.run());
                    }else{
                        runnable.run();
                    }
                })
                .withContext((uni, ctx) -> {
                    return uni
                            .invoke(m -> startSpan(tracer, parentContext, ctx, spanName))
                            .replaceWith(pipeline)
                            .eventually(() -> endSpanCloseScope(ctx));
                });
    }


    private static void startSpan(final Tracer tracer, final Optional<io.opentelemetry.context.Context> tracingData,
            final io.smallrye.mutiny.Context ctx, final String spanName) {
        final SpanBuilder spanBuilder = tracer.spanBuilder(spanName);
        if(tracingData.isPresent()){
            spanBuilder.setParent(tracingData.get());
        }else{
            spanBuilder.setNoParent();
        }


        final Span span = spanBuilder.startSpan();
        final Scope scope = QuarkusContextStorage.INSTANCE.attach(io.opentelemetry.context.Context.current().with(span));

        storeSpanAndScope(ctx, span, scope);
    }

    private static void storeSpanAndScope(final io.smallrye.mutiny.Context ctx, final Span span, final Scope scope) {
        final SpanAndScope spanAndScope = new SpanAndScope(span, scope);

        final Deque<SpanAndScope> stack = ctx.getOrElse(SPAN_STACK, LinkedList::new);
        stack.push(spanAndScope);
        ctx.put(SPAN_STACK, stack);
        ctx.put("CURRENT_SPAN", span);
        ctx.put("CURRENT_SCOPE", scope);
    }

    private static void endSpanCloseScope(final io.smallrye.mutiny.Context ctx) {
        final Deque<SpanAndScope> stack = ctx.getOrElse(SPAN_STACK, () -> null);
        if(stack != null && !stack.isEmpty()) {
            final SpanAndScope spanAndScope = stack.pop();
            spanAndScope.span().end();
            spanAndScope.scope().close();
        }
    }


    private record SpanAndScope(Span span, Scope scope) {
    }
}

