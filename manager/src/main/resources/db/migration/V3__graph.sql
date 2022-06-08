# Target version takes an input from a source dataset
CREATE TABLE graph (
    target_dataset_id BIGINT NOT NULL,
    source_dataset_id BIGINT NOT NULL,
    binding VARCHAR(32) NOT NULL,
    input_mode TEXT NOT NULL,
    valid BOOL NOT NULL,
    created_at BIGINT UNSIGNED NOT NULL,
    updated_at BIGINT UNSIGNED NOT NULL,

    FOREIGN KEY (target_dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
    FOREIGN KEY (source_dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,

    PRIMARY KEY (target_dataset_id, source_dataset_id),
    INDEX source_idx (source_dataset_id)
);
