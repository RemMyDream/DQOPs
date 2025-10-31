-- GDELT Events Table (simplified schema)
CREATE TABLE IF NOT EXISTS gdelt_events (
    global_event_id BIGINT PRIMARY KEY,
    event_date DATE NOT NULL,
    event_year INTEGER,
    event_month INTEGER,
    event_day INTEGER,
    actor1_code VARCHAR(20),
    actor1_name VARCHAR(255),
    actor1_country_code VARCHAR(3),
    actor1_type1_code VARCHAR(3),
    actor2_code VARCHAR(20),
    actor2_name VARCHAR(255),
    actor2_country_code VARCHAR(3),
    actor2_type1_code VARCHAR(3),
    event_code VARCHAR(4),
    event_base_code VARCHAR(4),
    event_root_code VARCHAR(4),
    quad_class INTEGER,
    goldstein_scale DECIMAL(5,2),
    num_mentions INTEGER,
    num_sources INTEGER,
    num_articles INTEGER,
    avg_tone DECIMAL(10,6),
    action_geo_type INTEGER,
    action_geo_fullname VARCHAR(255),
    action_geo_country_code VARCHAR(2),
    action_geo_lat DECIMAL(9,6),
    action_geo_long DECIMAL(9,6),
    date_added TIMESTAMP,
    source_url TEXT
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_gdelt_event_date ON gdelt_events(event_date);
CREATE INDEX IF NOT EXISTS idx_gdelt_actor1_country ON gdelt_events(actor1_country_code);
CREATE INDEX IF NOT EXISTS idx_gdelt_actor2_country ON gdelt_events(actor2_country_code);
CREATE INDEX IF NOT EXISTS idx_gdelt_event_code ON gdelt_events(event_code);

-- GDELT Mentions Table (references to events in articles)
CREATE TABLE IF NOT EXISTS gdelt_mentions (
    mention_id SERIAL PRIMARY KEY,
    global_event_id BIGINT REFERENCES gdelt_events(global_event_id),
    mention_time_date TIMESTAMP,
    mention_type INTEGER,
    mention_source_name VARCHAR(255),
    mention_identifier TEXT,
    sentence_id INTEGER,
    actor1_char_offset INTEGER,
    actor2_char_offset INTEGER,
    action_char_offset INTEGER,
    in_raw_text BOOLEAN,
    confidence INTEGER,
    mention_doc_len INTEGER,
    mention_doc_tone DECIMAL(10,6)
);

CREATE INDEX IF NOT EXISTS idx_mentions_event_id ON gdelt_mentions(global_event_id);
CREATE INDEX IF NOT EXISTS idx_mentions_time ON gdelt_mentions(mention_time_date);

-- SQuAD (Stanford Question Answering Dataset) inspired table
CREATE TABLE IF NOT EXISTS squad_data (
    id SERIAL PRIMARY KEY,
    title VARCHAR(500),
    context TEXT NOT NULL,
    question TEXT NOT NULL,
    answer_text TEXT,
    answer_start INTEGER,
    is_impossible BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_squad_title ON squad_data(title);

-- Insert dummy GDELT Events data
INSERT INTO gdelt_events (
    global_event_id, event_date, event_year, event_month, event_day,
    actor1_code, actor1_name, actor1_country_code, actor1_type1_code,
    actor2_code, actor2_name, actor2_country_code, actor2_type1_code,
    event_code, event_base_code, event_root_code, quad_class, goldstein_scale,
    num_mentions, num_sources, num_articles, avg_tone,
    action_geo_type, action_geo_fullname, action_geo_country_code,
    action_geo_lat, action_geo_long, date_added, source_url
) VALUES
(1000001, '2024-01-15', 2024, 1, 15, 'USA', 'UNITED STATES', 'US', 'GOV',
 'CHN', 'CHINA', 'CN', 'GOV', '043', '043', '04', 1, 4.0,
 25, 15, 20, -2.5, 1, 'United States', 'US', 38.8951, -77.0364,
 '2024-01-15 10:30:00', 'https://example.com/news1'),

(1000002, '2024-01-16', 2024, 1, 16, 'RUS', 'RUSSIA', 'RU', 'GOV',
 'UKR', 'UKRAINE', 'UA', 'GOV', '190', '190', '19', 4, -5.0,
 150, 75, 100, -8.2, 1, 'Kyiv, Ukraine', 'UA', 50.4501, 30.5234,
 '2024-01-16 14:20:00', 'https://example.com/news2'),

(1000003, '2024-01-17', 2024, 1, 17, 'FRA', 'FRANCE', 'FR', 'GOV',
 'DEU', 'GERMANY', 'DE', 'GOV', '036', '036', '03', 1, 6.0,
 40, 25, 35, 3.8, 1, 'Brussels, Belgium', 'BE', 50.8503, 4.3517,
 '2024-01-17 09:15:00', 'https://example.com/news3'),

(1000004, '2024-01-18', 2024, 1, 18, 'ISR', 'ISRAEL', 'IL', 'GOV',
 'PSE', 'PALESTINIAN AUTHORITY', 'PS', 'GOV', '175', '175', '17', 4, -6.5,
 200, 100, 150, -9.5, 1, 'Gaza Strip', 'PS', 31.3547, 34.3088,
 '2024-01-18 16:45:00', 'https://example.com/news4'),

(1000005, '2024-01-19', 2024, 1, 19, 'JPN', 'JAPAN', 'JP', 'GOV',
 'KOR', 'SOUTH KOREA', 'KR', 'GOV', '051', '051', '05', 1, 5.5,
 30, 20, 25, 4.2, 1, 'Seoul, South Korea', 'KR', 37.5665, 126.9780,
 '2024-01-19 11:00:00', 'https://example.com/news5'),

(1000006, '2024-01-20', 2024, 1, 20, 'BRA', 'BRAZIL', 'BR', 'GOV',
 'ARG', 'ARGENTINA', 'AR', 'GOV', '042', '042', '04', 1, 4.5,
 35, 22, 28, 2.8, 1, 'Buenos Aires, Argentina', 'AR', -34.6037, -58.3816,
 '2024-01-20 13:30:00', 'https://example.com/news6'),

(1000007, '2024-01-21', 2024, 1, 21, 'IND', 'INDIA', 'IN', 'GOV',
 'PAK', 'PAKISTAN', 'PK', 'GOV', '112', '112', '11', 3, -2.0,
 80, 50, 65, -5.3, 1, 'Kashmir', 'IN', 34.0837, 74.7973,
 '2024-01-21 18:20:00', 'https://example.com/news7'),

(1000008, '2024-01-22', 2024, 1, 22, 'GBR', 'UNITED KINGDOM', 'GB', 'GOV',
 'EUR', 'EUROPEAN UNION', 'EU', 'IGO', '031', '031', '03', 1, 3.5,
 45, 30, 38, 1.5, 1, 'London, United Kingdom', 'GB', 51.5074, -0.1278,
 '2024-01-22 08:45:00', 'https://example.com/news8'),

(1000009, '2024-01-23', 2024, 1, 23, 'AUS', 'AUSTRALIA', 'AU', 'GOV',
 'CHN', 'CHINA', 'CN', 'GOV', '129', '129', '12', 3, -1.5,
 55, 35, 45, -3.2, 1, 'Canberra, Australia', 'AU', -35.2809, 149.1300,
 '2024-01-23 12:10:00', 'https://example.com/news9'),

(1000010, '2024-01-24', 2024, 1, 24, 'CAN', 'CANADA', 'CA', 'GOV',
 'USA', 'UNITED STATES', 'US', 'GOV', '040', '040', '04', 1, 5.0,
 28, 18, 22, 3.5, 1, 'Ottawa, Canada', 'CA', 45.4215, -75.6972,
 '2024-01-24 10:00:00', 'https://example.com/news10');

-- Insert dummy GDELT Mentions data
INSERT INTO gdelt_mentions (
    global_event_id, mention_time_date, mention_type, mention_source_name,
    mention_identifier, sentence_id, confidence, mention_doc_tone
) VALUES
(1000001, '2024-01-15 10:30:00', 1, 'Reuters', 'https://reuters.com/article1', 1, 75, -2.5),
(1000001, '2024-01-15 11:45:00', 1, 'CNN', 'https://cnn.com/article1', 2, 80, -2.0),
(1000002, '2024-01-16 14:20:00', 1, 'BBC', 'https://bbc.com/article2', 1, 90, -8.5),
(1000002, '2024-01-16 15:30:00', 1, 'Al Jazeera', 'https://aljazeera.com/article2', 3, 85, -8.0),
(1000003, '2024-01-17 09:15:00', 1, 'Le Monde', 'https://lemonde.fr/article3', 1, 70, 4.0),
(1000004, '2024-01-18 16:45:00', 1, 'Haaretz', 'https://haaretz.com/article4', 2, 88, -9.0),
(1000005, '2024-01-19 11:00:00', 1, 'Asahi Shimbun', 'https://asahi.com/article5', 1, 82, 4.5),
(1000006, '2024-01-20 13:30:00', 1, 'Folha', 'https://folha.com/article6', 1, 76, 3.0),
(1000007, '2024-01-21 18:20:00', 1, 'Times of India', 'https://timesofindia.com/article7', 2, 79, -5.0),
(1000008, '2024-01-22 08:45:00', 1, 'Guardian', 'https://theguardian.com/article8', 1, 84, 2.0);

-- Insert dummy SQuAD data
INSERT INTO squad_data (title, context, question, answer_text, answer_start, is_impossible) VALUES
('Climate Change', 
 'Climate change refers to long-term shifts in temperatures and weather patterns. These shifts may be natural, but since the 1800s, human activities have been the main driver of climate change, primarily due to the burning of fossil fuels like coal, oil and gas.',
 'What is the main driver of climate change since the 1800s?',
 'human activities',
 138,
 FALSE),

('Artificial Intelligence',
 'Artificial intelligence is the simulation of human intelligence processes by machines, especially computer systems. These processes include learning, reasoning, and self-correction. Applications of AI include expert systems, natural language processing, speech recognition and machine vision.',
 'What does AI stand for?',
 'Artificial intelligence',
 0,
 FALSE),

('Renewable Energy',
 'Renewable energy is energy from sources that are naturally replenishing but flow-limited. They are virtually inexhaustible in duration but limited in the amount of energy that is available per unit of time. The major types of renewable energy sources are solar, wind, hydroelectric, biomass, and geothermal.',
 'What are the major types of renewable energy?',
 'solar, wind, hydroelectric, biomass, and geothermal',
 285,
 FALSE),

('Machine Learning',
 'Machine learning is a subset of artificial intelligence that enables systems to learn and improve from experience without being explicitly programmed. It focuses on the development of computer programs that can access data and use it to learn for themselves.',
 'What is machine learning a subset of?',
 'artificial intelligence',
 31,
 FALSE),

('Quantum Computing',
 'Quantum computing harnesses the phenomena of quantum mechanics to deliver a huge leap forward in computation to solve certain problems. Quantum computers use quantum bits, or qubits, which can exist in multiple states simultaneously.',
 'What do quantum computers use instead of regular bits?',
 'quantum bits, or qubits',
 144,
 FALSE);

-- Grant permissions (if needed)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO spark;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO spark;