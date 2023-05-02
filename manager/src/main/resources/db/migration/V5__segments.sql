CREATE TABLE segments (
    id BIGINT NOT NULL AUTO_INCREMENT,
    dataset_id BIGINT NOT NULL,
    version BIGINT NOT NULL,
    state ENUM('Initializing', 'Announced', 'Queued', 'Awaiting', 'Transforming', 'Materialized', 'Failed', 'Released') NOT NULL,
    created_at BIGINT UNSIGNED NOT NULL,
    updated_at BIGINT UNSIGNED NOT NULL,

    FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,

    PRIMARY KEY (id),
    UNIQUE INDEX (dataset_id, version)
);

# Target version takes as input a source version (with a binding)
CREATE TABLE segment_inputs (
    target_segment_id BIGINT NOT NULL,
    source_segment_id BIGINT NOT NULL,
    binding VARCHAR(32) NOT NULL,

    FOREIGN KEY (target_segment_id) REFERENCES segments(id) ON DELETE CASCADE,
    FOREIGN KEY (source_segment_id) REFERENCES segments(id) ON DELETE CASCADE,

    PRIMARY KEY (target_segment_id, source_segment_id, binding),
    INDEX source_idx (source_segment_id)
);

# Information about materialized segments
CREATE TABLE segment_data (
    segment_id BIGINT NOT NULL,
    path TEXT NOT NULL,
    checksum VARCHAR(64) NOT NULL,
    size BIGINT NOT NULL,
    row_count BIGINT NOT NULL,
    sample TEXT NOT NULL,

    FOREIGN KEY (segment_id) REFERENCES segments(id) ON DELETE CASCADE,
    PRIMARY KEY (segment_id)
);


CREATE TABLE segment_transitions (
    segment_id BIGINT NOT NULL,
    transition TEXT NOT NULL,
    created_at BIGINT NOT NULL,

    FOREIGN KEY (segment_id) REFERENCES segments(id) ON DELETE CASCADE,
    INDEX (segment_id, created_at)
)