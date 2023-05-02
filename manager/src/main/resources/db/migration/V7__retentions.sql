CREATE TABLE segment_retentions(
    segment_id BIGINT NOT NULL,
    holder BIGINT NOT NULL,
    state ENUM('Initializing', 'Announced', 'Queued', 'Awaiting', 'Transforming', 'Materialized', 'Failed', 'Released') NOT NULL,
    shallow_size BIGINT NOT NULL,
    cause VARCHAR(255) NOT NULL,

    FOREIGN KEY (segment_id) REFERENCES segments(id) ON DELETE CASCADE,
    FOREIGN KEY (holder) REFERENCES entities(id) ON DELETE CASCADE,

    INDEX by_holder(holder, shallow_size DESC, segment_id),
    INDEX by_segment(segment_id, shallow_size DESC, holder),

    UNIQUE(segment_id, holder, cause)
);
