CREATE TABLE datasets (
    id BIGINT NOT NULL AUTO_INCREMENT,
    owner_id BIGINT NOT NULL,
    tag VARCHAR(32) NOT NULL,
    description TEXT NOT NULL,
    datatype TEXT NOT NULL,
    visibility ENUM('Public', 'Private'),
    retention_ms BIGINT NULL,
    schedule_ms BIGINT NULL,
    preferred_cluster BIGINT NULL,
    created_at BIGINT UNSIGNED NOT NULL,
    updated_at BIGINT UNSIGNED NOT NULL,

    FOREIGN KEY (preferred_cluster) REFERENCES clusters(id) ON DELETE SET NULL,
    FOREIGN KEY (owner_id) REFERENCES entities(id) ON DELETE CASCADE,
    PRIMARY KEY (id),
    UNIQUE INDEX tag_idx (owner_id, tag),
    INDEX age_idx (created_at)
);

CREATE TABLE dataset_acl (
    dataset_id BIGINT NOT NULL,
    entity_id BIGINT NOT NULL,
    role ENUM('Owner', 'Member') NOT NULL,

    FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    PRIMARY KEY (dataset_id, entity_id)
);

CREATE TABLE ingestions (
    dataset_id BIGINT NOT NULL AUTO_INCREMENT,
    mode ENUM('MANUAL', 'SCHEDULED') NOT NULL,
    created_at BIGINT UNSIGNED NOT NULL,
    updated_at BIGINT UNSIGNED NOT NULL,

    FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
    PRIMARY KEY (dataset_id),
    INDEX age_idx (created_at)
);

CREATE TABLE ingestion_schedules (
    dataset_id BIGINT NOT NULL,
    job_payload TEXT NOT NULL,

    FOREIGN KEY (dataset_id) REFERENCES ingestions(dataset_id) ON DELETE CASCADE,
    PRIMARY KEY (dataset_id)
);
