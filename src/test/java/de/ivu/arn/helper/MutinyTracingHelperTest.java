package de.ivu.arn.helper;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.quarkus.opentelemetry.runtime.QuarkusContextStorage;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.common.vertx.VertxContext;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import jakarta.inject.Inject;

@QuarkusTest
class MutinyTracingHelperTest {

    private static final Logger LOG = LoggerFactory.getLogger(MutinyTracingHelperTest.class);

    MutinyTracingHelper mutinyTracingHelper;

    private InMemorySpanExporter spanExporter;
    private Tracer tracer;

    @Inject
    Vertx vertx;

    @BeforeEach
    public void setup() {
        GlobalOpenTelemetry.resetForTest();

        spanExporter = InMemorySpanExporter.create();
        final SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).buildAndRegisterGlobal();

        tracer = GlobalOpenTelemetry.getTracer(MutinyTracingHelper.class.getName());

        mutinyTracingHelper = new MutinyTracingHelper(tracer);
    }

    @ParameterizedTest(name = "{index}: Simple uni pipeline {1}")
    @MethodSource("generateContextRunners")
    void testSimpleUniPipeline(final ContextProviderRunner contextRunner, final String contextName) {

        final UniAssertSubscriber<String> subscriber = Uni.createFrom()
                .item("Hello")
                .emitOn(r -> contextRunner.runOnContext(r, vertx))
                .onItem()
                .transformToUni(m -> mutinyTracingHelper.wrapWithSpan(Optional.empty(), "testSpan",
                        Uni.createFrom().item(m).onItem().transform(s -> {
                            final Span span = tracer.spanBuilder("subspan").startSpan();
                            try (final Scope scope = span.makeCurrent()) {
                                return s + " world";
                            } finally {
                                span.end();
                            }
                        })))
                .subscribe()
                .withSubscriber(new UniAssertSubscriber<>());

        subscriber.awaitItem().assertItem("Hello world");

        //ensure there are two spans with subspan as child of testSpan
        final List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).hasSize(2);
        assertThat(spans.stream().anyMatch(span -> span.getName().equals("testSpan"))).isTrue();
        assertThat(spans.stream().anyMatch(span -> span.getName().equals("subspan"))).isTrue();
        assertThat(spans.stream()
                .filter(span -> span.getName().equals("subspan"))
                .findAny()
                .orElseThrow()
                .getParentSpanId()).isEqualTo(
                spans.stream().filter(span -> span.getName().equals("testSpan")).findAny().get().getSpanId());
    }

    @ParameterizedTest(name = "{index}: Nested uni pipeline {1}")
    @MethodSource("generateContextRunners")
    void testNestedUniPipeline(final ContextProviderRunner contextRunner, final String contextName) {

        final UniAssertSubscriber<Void> subscriber = Uni.createFrom()
                .item("test")
                .emitOn(r -> contextRunner.runOnContext(r, vertx))
                .onItem()
                .transformToUni(m -> mutinyTracingHelper.wrapWithSpan("testSpan",
                        Uni.createFrom().item(m)
                                .onItem().invoke(s -> doSomething(s))
                                .onItem().transformToUni(s -> doSomethingAsync(s))

                ))
                .subscribe()
                .withSubscriber(new UniAssertSubscriber<>());

        subscriber.awaitItem();

        //ensure there are 3 spans with doSomething and doSomethingAsync as children of testSpan
        final List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).hasSize(3);
        assertThat(spans.stream().map(SpanData::getName)).containsExactlyInAnyOrder("testSpan", "doSomething", "doSomethingAsync");
        assertThat(spans.stream()
                .filter(span -> span.getName().equals("doSomething"))
                .findAny()
                .orElseThrow()
                .getParentSpanId()).isEqualTo(
                spans.stream().filter(span -> span.getName().equals("testSpan")).findAny().get().getSpanId());
        assertThat(spans.stream()
                .filter(span -> span.getName().equals("doSomethingAsync"))
                .findAny()
                .orElseThrow()
                .getParentSpanId()).isEqualTo(
                spans.stream().filter(span -> span.getName().equals("testSpan")).findAny().get().getSpanId());
    }

    public void doSomething(final String s){
        final Span span = tracer.spanBuilder("doSomething").startSpan();
        try (final Scope scope = span.makeCurrent()) {
            LOG.info("do something with {}", s);
        } finally {
            span.end();
        }
    }

    public Uni<Void> doSomethingAsync(final String string){
        return Uni.createFrom().item(string)
                .onItem().transformToUni(m -> mutinyTracingHelper.wrapWithSpan(Optional.of(
                                io.opentelemetry.context.Context.current()), "doSomethingAsync",
                        Uni.createFrom().item(m)
                                .onItem().invoke(s -> LOG.info("do something async with {}", s))
                ))
                .replaceWithVoid();
    }

    @ParameterizedTest(name = "{index}: Concatenating multi pipeline {1}")
    @MethodSource("generateContextRunners")
    void testSimpleMultiPipeline_Concatenate(final ContextProviderRunner contextRunner, final String contextName) {

        final AssertSubscriber<String> subscriber = Multi.createFrom()
                .items("test1", "test2", "test3")
                .emitOn(r -> contextRunner.runOnContext(r, vertx))
                .onItem()
                .transformToUniAndConcatenate(m -> mutinyTracingHelper.wrapWithSpan(Optional.empty(), "testSpan " + m,
                        //the traced pipeline
                        Uni.createFrom().item(m).onItem().transform(s -> {
                            final Span span = tracer.spanBuilder("subspan " + s).startSpan();
                            try (final Scope scope = span.makeCurrent()) {
                                return s + " transformed";
                            } finally {
                                span.end();
                            }
                        })))
                .subscribe()
                .withSubscriber(AssertSubscriber.create(3));

        subscriber.awaitCompletion().assertItems("test1 transformed", "test2 transformed", "test3 transformed");

        //ensure there are six spans with three pairs of subspan as child of testSpan
        final List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).hasSize(6);
        for(int i = 1; i <= 3; i++){
            final int currentI = i;
            assertThat(spans.stream().anyMatch(span -> span.getName().equals("testSpan test" + currentI))).isTrue();
            assertThat(spans.stream().anyMatch(span -> span.getName().equals("subspan test" + currentI))).isTrue();
            assertThat(spans.stream()
                    .filter(span -> span.getName().equals("subspan test" + currentI))
                    .findAny()
                    .orElseThrow()
                    .getParentSpanId()).isEqualTo(
                    spans.stream().filter(span -> span.getName().equals("testSpan test" + currentI)).findAny().get().getSpanId());
        }
    }

    @ParameterizedTest(name = "{index}: Merging multi pipeline {1}")
    @MethodSource("generateContextRunners")
    void testSimpleMultiPipeline_Merge(final ContextProviderRunner contextRunner, final String contextName) {

        final AssertSubscriber<String> subscriber = Multi.createFrom()
                .items("test1", "test2", "test3")
                .emitOn(r -> contextRunner.runOnContext(r, vertx))
                .onItem()
                .transformToUniAndMerge(m -> mutinyTracingHelper.wrapWithSpan(Optional.empty(), "testSpan " + m,
                        Uni.createFrom().item(m).onItem().transform(s -> {
                            final Span span = tracer.spanBuilder("subspan " + s).startSpan();
                            try (final Scope scope = span.makeCurrent()) {
                                return s + " transformed";
                            } finally {
                                span.end();
                            }
                        })))
                .subscribe()
                .withSubscriber(AssertSubscriber.create(3));

        subscriber.awaitCompletion();

        //ensure there are six spans with three pairs of subspan as child of testSpan
        final List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).hasSize(6);
        for(int i = 1; i <= 3; i++){
            final int currentI = i;
            assertThat(spans.stream().anyMatch(span -> span.getName().equals("testSpan test" + currentI))).isTrue();
            assertThat(spans.stream().anyMatch(span -> span.getName().equals("subspan test" + currentI))).isTrue();
            assertThat(spans.stream()
                    .filter(span -> span.getName().equals("subspan test" + currentI))
                    .findAny()
                    .orElseThrow()
                    .getParentSpanId()).isEqualTo(
                    spans.stream().filter(span -> span.getName().equals("testSpan test" + currentI)).findAny().get().getSpanId());
        }
    }



    private static Stream<Arguments> generateContextRunners() {
        return Stream.of(
                Arguments.of(new WithoutContextRunner(), "Without Context"),
                Arguments.of(new RootContextRunner(), "On Root Context"),
                Arguments.of(new DuplicatedContextRunner(), "On Duplicated Context")
        );
    }


    private interface ContextProviderRunner{
        void runOnContext(Runnable runnable, Vertx vertx);
    }

    private static class WithoutContextRunner implements ContextProviderRunner {

        @Override
        public void runOnContext(final Runnable runnable, final Vertx vertx) {
            assertThat(QuarkusContextStorage.getVertxContext()).isNull();
            runnable.run();
        }
    }

    private static class RootContextRunner implements ContextProviderRunner {
        @Override
        public void runOnContext(final Runnable runnable, final Vertx vertx) {
            final Context rootContext = VertxContext.getRootContext(vertx.getOrCreateContext());
            assertThat(rootContext).isNotNull();
            assertThat(VertxContext.isDuplicatedContext(rootContext)).isFalse();
            assertThat(rootContext).isNotEqualTo(QuarkusContextStorage.getVertxContext());

            rootContext.runOnContext(v -> runnable.run());
        }
    }

    private static class DuplicatedContextRunner implements ContextProviderRunner {
        @Override
        public void runOnContext(final Runnable runnable, final Vertx vertx) {
            final Context duplicatedContext = VertxContext.createNewDuplicatedContext(vertx.getOrCreateContext());
            assertThat(duplicatedContext).isNotNull();
            assertThat(VertxContext.isDuplicatedContext(duplicatedContext)).isTrue();

            duplicatedContext.runOnContext(v -> runnable.run());
        }
    }

}