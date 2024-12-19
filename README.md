# mutiny-tracing-helper

This project uses Quarkus, the Supersonic Subatomic Java Framework.

If you want to learn more about Quarkus, please visit its website: <https://quarkus.io/>.

## Purpose
Support for tracing with custom spans on mutiny pipelines.
Inspired by https://github.com/jan-peremsky/quarkus-reactive-otel/blob/c74043d388ec4df155f466f1d6938931c3389b70/src/main/java/com/fng/ewallet/pex/Tracer.java

## Usage
see also MutinyTracingHelperTest.java

### Uni
Assuming you have the following pipeline:
```java
Uni<String> uni = Uni.createFrom().item("hello")
        //start trace here
        .onItem().transform(item -> item + " world")
        .onItem().transform(item -> item + "!")
        //end trace here
        .subscribe().with(
                item -> System.out.println("Received: " + item),
                failure -> System.out.println("Failed with " + failure)
        );
```
wrap the pipeline like this:
```java
Uni<String> uni = Uni.createFrom().item("hello")
        .transformToUni(m -> TracingHelper.withTrace("my-span-name", 
                                Uni.createFrom().item(m)
                                    .onItem().transform(item -> item + " world")
                                    .onItem().transform(item -> item + "!")
        ))
        .subscribe().with(
                item -> System.out.println("Received: " + item),
                failure -> System.out.println("Failed with " + failure)
        );
 ```
### Multi
It also works for multis:
```java
Multi.createFrom().items("Alice", "Bob", "Charlie")
        //start trace here
        .onItem().transform(name -> "Hello " + name)
        //end trace here
        .subscribe().with(
                item -> System.out.println("Received: " + item),
                failure -> System.out.println("Failed with " + failure)
        );
```
wrap the pipeline like this:
```java
Multi.createFrom().items("Alice", "Bob", "Charlie")
        .transformToMultiAndConcatenate(m -> TracingHelper.withTrace("my-span-name", 
                                Multi.createFrom().item(m)
                                    .onItem().transform(name -> "Hello " + name)
        ))
        .subscribe().with(
                item -> System.out.println("Received: " + item),
                failure -> System.out.println("Failed with " + failure)
        );
```
instead of `transformToMultiAndConcatenate` you can use `transformToMultiAndMerge` if you don't care about the order of the items.

## Related Guides

- OpenTelemetry ([guide](https://quarkus.io/guides/opentelemetry)): Use OpenTelemetry to trace services
