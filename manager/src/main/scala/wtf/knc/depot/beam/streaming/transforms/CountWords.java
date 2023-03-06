package wtf.knc.depot.beam.streaming.transforms;

import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * A PTransform that converts a PCollection containing lines of text into a PCollection of
 * formatted word counts.
 *
 */
public class CountWords
        extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {
        // Convert lines of text into individual words.

        PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

        // Count the number of times each word occurs.
        PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());



        return wordCounts;
    }
}