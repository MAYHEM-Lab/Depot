package com.sunil.transforms;

import org.apache.beam.sdk.transforms.DoFn;

public class ExtractWordsFn extends DoFn<String, String> {
    public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {
        // Split the line into words.
        String[] words = element.split(TOKENIZER_PATTERN, -1);

        // Output each word encountered into the output PCollection.
        for (String word : words) {
            if (!word.isEmpty()) {
                receiver.output(word);
            }
        }
    }
}
