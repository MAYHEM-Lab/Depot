package wtf.knc.depot.beam.streaming;

import com.google.gson.Gson;
import wtf.knc.depot.beam.streaming.coders.RecordSerializableCoder;
import wtf.knc.depot.beam.streaming.loggers.WriteOneFilePerWindow;
import wtf.knc.depot.beam.streaming.models.FileResponse;
import wtf.knc.depot.beam.streaming.objects.Record;
import wtf.knc.depot.beam.streaming.policies.CustomFieldTimePolicy;
import wtf.knc.depot.beam.streaming.serde.RecordDeserializer;
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
import wtf.knc.depot.beam.streaming.transforms.CountWords;
import wtf.knc.depot.beam.streaming.transforms.WriteToDepot;

import java.io.IOException;
import java.util.UUID;

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
                .apply("format result to String", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Long> rec) -> rec.getKey() + ":" + rec.getValue())).apply(
                        "transform", ParDo.of(new DoFn<String, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c, OutputReceiver<String> receiver) {
                                try {
                                    FileResponse fileResponse = WriteToDepot.post("http://localhost:3000/api/entity/samridhim/files", "{\"parts\": \"1\", \"content_type\":\"text/plain\"}");
                                    String uploadId = fileResponse.getId();
                                    String name = fileResponse.getFilename();
                                    System.out.println("File Response:" + fileResponse.getParts().get(0));
                                    int uploadResponse = WriteToDepot.upload("http://localhost:3000"+fileResponse.getParts().get(0), c.element());
                                    System.out.println("Uploading File to S3 Response:" + uploadResponse);
                                    fileResponse = WriteToDepot.post("http://localhost:3000/api/entity/samridhim/files/"+fileResponse.getFilename(), "{\"upload_id\":\""+uploadId+"\"}");
                                    String datasetName = "streaming-data-" + String.valueOf(System.currentTimeMillis());
                                    int responseCode = WriteToDepot.createDataset("http://localhost:3000/api/entity/samridhim/datasets/"+datasetName);
                                    System.out.println("response of create dataset: " + responseCode);
                                    fileResponse = WriteToDepot.post("http://localhost:3000/api/entity/samridhim/datasets/"+datasetName+"/upload", "{\"files\": {\"abc.txt\": \""+name+"\"}}");
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                System.out.println(c.element());
                                receiver.output(c.element());
                            }
                        })
                )
                .apply("Write it to a text file", new WriteOneFilePerWindow(options.getOutput()));


        pipeline.run();
    }

    public static void runStream() {
        WindowedWordCountOptions options = PipelineOptionsFactory.as(WindowedWordCountOptions.class);
        options.setStreaming(true);
        runWithOptions(options);
    }
}
