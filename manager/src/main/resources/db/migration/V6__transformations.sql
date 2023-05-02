CREATE TABLE transformations(
    dataset_id BIGINT NOT NULL,
    notebook_tag VARCHAR(32) NOT NULL,

    FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
    FOREIGN KEY (notebook_tag) REFERENCES notebooks(tag) ON DELETE CASCADE,

    PRIMARY KEY (dataset_id)
);
