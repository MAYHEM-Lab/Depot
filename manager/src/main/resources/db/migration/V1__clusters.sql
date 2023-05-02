CREATE TABLE clusters(
    id BIGINT NOT NULL AUTO_INCREMENT,
    owner_id BIGINT NOT NULL,
    tag VARCHAR(32) NOT NULL,
    status ENUM('Provisioning', 'Active') NOT NULL,
    created_at BIGINT UNSIGNED NOT NULL,
    updated_at BIGINT UNSIGNED NOT NULL,

    FOREIGN KEY (owner_id) REFERENCES entities(id) ON DELETE CASCADE,
    PRIMARY KEY (id)
);

CREATE TABLE spark_info(
    cluster_id BIGINT NOT NULL,
    spark_master TEXT NOT NULL,

    FOREIGN KEY (cluster_id) REFERENCES clusters(id) ON DELETE CASCADE,
    PRIMARY KEY (cluster_id)
);

CREATE TABLE notebook_info(
    cluster_id BIGINT NOT NULL,
    notebook_master TEXT NOT NULL,

    FOREIGN KEY (cluster_id) REFERENCES clusters(id) ON DELETE CASCADE,
    PRIMARY KEY (cluster_id)
);

CREATE TABLE transformer_info(
    cluster_id BIGINT NOT NULL,
    transformer TEXT NOT NULL,

    FOREIGN KEY (cluster_id) REFERENCES clusters(id) ON DELETE CASCADE,
    PRIMARY KEY (cluster_id)
);
