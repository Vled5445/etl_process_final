-- Database etl_db

CREATE TABLE user_sessions (
    session_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    pages_visited TEXT[],
    device_mobile BOOLEAN,
    device_desktop BOOLEAN,
    device_tablet BOOLEAN,
    device_other BOOLEAN,
    actions TEXT[],
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE event_logs (
    event_id VARCHAR(50) PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    details TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE support_tickets (
    ticket_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    issue_type VARCHAR(50) NOT NULL,
    messages TEXT[],
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE user_recommendations (
    user_id VARCHAR(50) PRIMARY KEY,
    recommended_products TEXT[],
    last_updated TIMESTAMP NOT NULL
);

CREATE TABLE moderation_queue (
    review_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    review_text TEXT,
    rating INTEGER,
    moderation_status VARCHAR(50) NOT NULL,
    flags TEXT[],
    submitted_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_user_sessions_user ON user_sessions(user_id);
CREATE INDEX idx_user_sessions_time ON user_sessions(start_time);
CREATE INDEX idx_event_logs_time ON event_logs(timestamp);
CREATE INDEX idx_support_tickets_status ON support_tickets(status);
CREATE INDEX idx_moderation_status ON moderation_queue(moderation_status);
