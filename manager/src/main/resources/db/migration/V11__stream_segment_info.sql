DROP TABLE IF EXISTS consumer_info;
CREATE TABLE consumer_info(
    cluster_id BIGINT NOT NULL,
    consumer TEXT NOT NULL,

    FOREIGN KEY (cluster_id) REFERENCES clusters(id) ON DELETE CASCADE,
    PRIMARY KEY (cluster_id)
);

DROP TABLE IF EXISTS streaming_segment_info;
CREATE TABLE streaming_segment_info(
    id BIGINT NOT NULL AUTO_INCREMENT,
    dataset_id BIGINT NOT NULL,
    segment_id BIGINT NOT NULL,
    start_offset BIGINT NOT NULL,
    end_offset   BIGINT NOT NULL,
    topic VARCHAR(32) NOT NULL,
    notebook_tag VARCHAR(32) NOT NULL,
    segment_version   BIGINT NOT NULL,
    bootstrap_server MEDIUMTEXT NOT NULL,
    FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
    FOREIGN KEY (notebook_tag) REFERENCES notebooks(tag) ON DELETE CASCADE,

    PRIMARY KEY (id)
);
