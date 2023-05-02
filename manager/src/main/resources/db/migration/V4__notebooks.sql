CREATE TABLE notebooks(
    tag VARCHAR(32) NOT NULL,
    owner_id BIGINT NOT NULL,
    created_at BIGINT UNSIGNED NOT NULL,
    updated_at BIGINT UNSIGNED NOT NULL,

    FOREIGN KEY (owner_id) REFERENCES entities(id) ON DELETE CASCADE,
    PRIMARY KEY (tag),
    UNIQUE INDEX tag_idx (owner_id, tag),
    INDEX age_idx (created_at)
);
