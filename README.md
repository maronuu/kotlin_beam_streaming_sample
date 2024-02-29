# kotlin_beam_streaming_sample
A digital signage application (without visualized UI) using Apache Beam and Apache Kafka in Kotlin.

We use Apache Beam to construct a streaming pipeline and Apache Kafka as the source and sink of the pipeline.

- `PassthroughPipeline`: The most straightforward pipeline that handles the row data.
- `SlidingWindowPipeline`: The pipeline that aggregates the values within a sliding window.


### Reference
- Testing datasets are from https://github.com/LambdaNote/support-stream-processing/tree/main/dataset-amedas.
- n月刊ラムダノート Vol.4, No.1(2024), ラムダノート, https://www.lambdanote.com/collections/n/products/nmonthly-vol-4-no-1-2024
- https://kafka.apache.org/quickstart
