CREATE TABLE entities (
    id BIGINT NOT NULL AUTO_INCREMENT,
    name VARCHAR(32) NOT NULL,
    type ENUM('User', 'Organization') NOT NULL,
    access_key TEXT NOT NULL,
    secret_key TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,

    PRIMARY KEY (id),
    UNIQUE INDEX (name)
);

INSERT INTO entities VALUES (0, 'root', 'Organization', '', '', 0, 0);

CREATE TABLE organization_members (
    organization_id BIGINT NOT NULL,
    member_id BIGINT NOT NULL,
    role ENUM('Owner', 'Member') NOT NULL,

    FOREIGN KEY (organization_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (member_id) REFERENCES entities(id) ON DELETE CASCADE,

    PRIMARY KEY (organization_id, member_id)
);

CREATE TABLE github_user_link (
    user_id BIGINT NOT NULL,
    github_id BIGINT NOT NULL,

    FOREIGN KEY (user_id) REFERENCES entities(id) ON DELETE CASCADE,

    PRIMARY KEY (user_id),
    UNIQUE INDEX (github_id)
);
