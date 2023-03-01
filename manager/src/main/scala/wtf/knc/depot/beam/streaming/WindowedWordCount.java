package wtf.knc.depot.beam.streaming;

import wtf.knc.depot.beam.streaming.coders.RecordSerializableCoder;
import wtf.knc.depot.beam.streaming.loggers.WriteOneFilePerWindow;
import wtf.knc.depot.beam.streaming.objects.Record;
import wtf.knc.depot.beam.streaming.policies.CustomFieldTimePolicy;
import wtf.knc.depot.beam.streaming.serde.RecordDeserializer;
import com.sunil.transforms.CountWords;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class WindowedWordCount {

    static void runWithOptions(wtf.knc.depot.beam.streaming.WindowedWordCountOptions options) {
        Pipeline pipeline = Pipeline.create(options);
        Duration WINDOW_TIME = Duration.standardMinutes(1);
        Duration ALLOWED_LATENESS = Duration.standardMinutes(1);
        Duration FIVE_MINUTES = Duration.standardMinutes(5);

        CoderRegistry cr = pipeline.getCoderRegistry();
        cr.registerCoderForClass(Record.class, new RecordSerializableCoder());


        pipeline.apply(
                KafkaIO.<Long, Record>read()
                        .withBootstrapServers(options.getBootstrap())
                        .withTopic(options.getInputTopic())
                        .withKeyDeserializer(LongDeserializer.class)
                        .withValueDeserializer(RecordDeserializer.class)
                        .withTimestampPolicyFactory((tp, previousWaterMark) -> new CustomFieldTimePolicy(previousWaterMark))
                        .withoutMetadata()
        )
                .apply(Values.<Record>create())
                .apply("append event time for PCollection records", WithTimestamps.of((Record rec) -> new Instant(rec.getEts())))
                .apply("extract message string", MapElements
                        .into(TypeDescriptors.strings())
                        .via(Record::getMessage))
                .apply("apply window", Window.<String>into(FixedWindows.of(WINDOW_TIME))
                        .withAllowedLateness(ALLOWED_LATENESS)
                        .triggering(AfterWatermark.pastEndOfWindow().withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1))).withLateFirings(AfterPane.elementCountAtLeast(1)))
                        .accumulatingFiredPanes())
                .apply("count words", new CountWords())
                .apply("format result to String",MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Long> rec) -> rec.getKey() + ":" + rec.getValue()))
                .apply("Write it to a text file", new WriteOneFilePerWindow(options.getOutput()));


        pipeline.run();
    }

//    public static void main(String[] args) {
//        WindowedWordCountOptions options = PipelineOptionsFactory.fromArgs(args).as(WindowedWordCountOptions.class);
//        options.setStreaming(true);
//        runWithOptions(options);
//    }

    public static void runStream() {
        WindowedWordCountOptions options = PipelineOptionsFactory.as(WindowedWordCountOptions.class);
        options.setStreaming(true);
        runWithOptions(options);
    }
}
