package wtf.knc.depot.beam.streaming;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;

public interface WindowedWordCountOptions extends StreamingOptions {

    @Default.String("localhost:9092")
    String getBootstrap();

    void setBootstrap(String value);

    @Default.String("messenger")
    String getInputTopic();

    void setInputTopic(String value);

    /**
     * Set this required option to specify where to write the output.
     */
    @Description("Path of the file to write to")
    @Validation.Required
    @Default.String("/Users/samridhi/kafka-output/")
    String getOutput();

    void setOutput(String value);
}