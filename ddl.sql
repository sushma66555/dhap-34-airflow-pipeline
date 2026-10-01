CREATE TABLE dataset_catalog (
    dataset_code INTEGER NOT NULL,
    dataset_name TEXT,
    link TEXT,
    description TEXT,
    comments TEXT,
    author TEXT,
    is_conversation_like TEXT,
    ai_generated TEXT,
    comments_on_relevance TEXT,
    analysis_author TEXT,
    analysis_author2 TEXT,
    analysis_status TEXT,
    analysis_status2 TEXT,
    PRIMARY KEY (dataset_code)
);