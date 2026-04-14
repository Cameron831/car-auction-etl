CREATE TABLE IF NOT EXISTS listings (
    id BIGSERIAL PRIMARY KEY,
    source_site TEXT NOT NULL,
    source_listing_id TEXT NOT NULL,
    url TEXT NOT NULL,
    make TEXT NOT NULL,
    model TEXT NOT NULL,
    year INTEGER NOT NULL,
    mileage INTEGER,
    vin TEXT,
    sale_price INTEGER NOT NULL,
    sold BOOLEAN NOT NULL,
    auction_end_date DATE NOT NULL,
    transmission TEXT,
    listing_details_raw JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_site, source_listing_id),
    CONSTRAINT listings_year_check CHECK (year >= 1886 AND year <= 2100),
    CONSTRAINT listings_mileage_check CHECK (mileage IS NULL OR mileage >= 0),
    CONSTRAINT listings_sale_price_check CHECK (sale_price >= 0),
    CONSTRAINT listings_transmission_check CHECK (transmission IS NULL OR transmission IN ('manual', 'automatic'))
);

