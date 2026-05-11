-- ========================================
-- Supabase Schema: Australian Leads Ingestion
-- Project: AAA Leads (8 Capital Cities)
-- ========================================

-- Enable required extensions
create extension if not exists "uuid-ossp";
create extension if not exists "pgcrypto";

-- ========================================
-- Table: leads
-- ========================================
create table if not exists leads (
    -- Primary key
    id uuid primary key default uuid_generate_v4(),

    -- Core identity
    lead_id varchar(255) unique not null,
    source varchar(100) not null,              -- e.g., 'google_business', 'yellow_pages', 'tradie_portal', 'manual'
    ingestion_batch_id uuid not null,          -- FK to ingestion_log

    -- Business details
    business_name varchar(500) not null,
    abn varchar(20),                           -- Australian Business Number (optional)
    category varchar(200) not null,            -- e.g., 'plumber', 'electrician', 'builder'
    subcategory varchar(200),
    services text,                             -- JSON array or comma-separated

    -- Contact
    phone varchar(50),
    mobile varchar(50),
    email varchar(255),
    website varchar(500),

    -- Location hierarchy
    country varchar(100) default 'Australia',
    state varchar(100) not null,               -- NSW, VIC, QLD, WA, SA, TAS, NT, ACT
    city varchar(100) not null,
    suburb varchar(200),
    postcode varchar(10),
    address_full text,
    geo_lat decimal(10, 7),
    geo_lng decimal(10, 7),

    -- Business metadata
    years_in_business integer,
    employee_count integer,
    rating decimal(3, 2),                      -- 0.00 - 5.00
    review_count integer default 0,

    -- Lead scoring / quality
    lead_score integer default 0,              -- 0-100 composite score
    tier varchar(20) default 'standard',       -- 'premium', 'standard', 'basic'
    is_active boolean default true,

    -- Timestamps
    first_seen_at timestamp with time zone default now(),
    last_verified_at timestamp with time zone,
    created_at timestamp with time zone default now(),
    updated_at timestamp with time zone default now(),

    -- Constraints
    constraint leads_city_state_check check (city is not null and state is not null),
    constraint leads_rating_check check (rating is null or (rating >= 0 and rating <= 5)),
    constraint leads_lead_score_check check (lead_score >= 0 and lead_score <= 100)
);

-- Indexes for fast queries
create index if not exists idx_leads_source on leads(source);
create index if not exists idx_leads_state_city on leads(state, city);
create index if not exists idx_leads_category on leads(category);
create index if not exists idx_leads_lead_score on leads(lead_score desc);
create index if not exists idx_leads_created_at on leads(created_at desc);
create index if not exists idx_leads_business_name on leads(business_name varchar_pattern_ops);
create index if not exists idx_leads_email on leads(email) where email is not null;
create index if not exists idx_leads_phone on leads(phone) where phone is not null;
create index if not exists idx_leads_geo on leads(geo_lat, geo_lng) where geo_lat is not null;

-- ========================================
-- Table: ingestion_log
-- ========================================
create table if not exists ingestion_log (
    -- Primary key
    id uuid primary key default uuid_generate_v4(),

    -- Ingestion identity
    batch_id varchar(100) unique not null,
    source_name varchar(200) not null,          -- e.g., 'google_maps_api', 'yellow_pages_scrape'
    city_target varchar(100) not null,          -- City being ingested
    state_target varchar(100) not null,         -- State being ingested
    record_count integer not null,              -- Total records attempted

    -- Status tracking
    status varchar(50) not null default 'running',  -- 'running', 'completed', 'failed', 'partial'
    records_inserted integer default 0,
    records_updated integer default 0,
    records_skipped integer default 0,
    records_failed integer default 0,

    -- Error capture
    error_summary text,
    error_details jsonb,

    -- Metadata
    source_config jsonb,                         -- Config used for this ingestion run
    duration_seconds integer,

    -- Timestamps
    started_at timestamp with time zone default now(),
    completed_at timestamp with time zone,

    -- Constraints
    constraint ingestion_status_check check (status in ('running', 'completed', 'failed', 'partial'))
);

-- Indexes
create index if not exists idx_ingestion_status on ingestion_log(status);
create index if not exists idx_ingestion_city on ingestion_log(city_target);
create index if not exists idx_ingestion_started on ingestion_log(started_at desc);
create index if not exists idx_ingestion_source on ingestion_log(source_name);

-- ========================================
-- Row Level Security (optional, if using auth)
-- ========================================
-- alter table leads enable row level security;
-- alter table ingestion_log enable row level security;

-- ========================================
-- Triggers
-- ========================================
-- Auto-update updated_at on leads
create or replace function update_updated_at_column()
returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language 'plpgsql';

create trigger update_leads_updated_at
    before update on leads
    for each row
    execute function update_updated_at_column();

-- ========================================
-- Views for analytics
-- ========================================
create or replace view v_leads_summary as
select
    state,
    city,
    category,
    count(*) as total_leads,
    count(*) filter (where is_active = true) as active_leads,
    avg(lead_score) as avg_lead_score,
    avg(rating) as avg_rating,
    min(created_at) as first_ingested,
    max(created_at) as last_ingested
from leads
group by state, city, category
order by state, city, total_leads desc;

create or replace view v_ingestion_dashboard as
select
    date_trunc('day', started_at) as ingestion_date,
    source_name,
    city_target,
    status,
    sum(record_count) as total_records,
    sum(records_inserted) as total_inserted,
    sum(records_failed) as total_failed,
    avg(duration_seconds) as avg_duration_secs,
    count(*) as batch_count
from ingestion_log
group by date_trunc('day', started_at), source_name, city_target, status
order by ingestion_date desc;

-- ========================================
-- Functions
-- ========================================
-- Get lead count by city
create or replace function get_leads_by_city(p_city varchar)
returns table (
    city varchar,
    lead_count bigint,
    avg_score numeric
) language sql stable as $$
    select
        city,
        count(*)::bigint as lead_count,
        round(avg(lead_score), 2) as avg_score
    from leads
    where city = p_city
    group by city;
$$;

-- Get ingestion stats
create or replace function get_ingestion_stats(p_days integer default 7)
returns table (
    total_batches bigint,
    successful_batches bigint,
    failed_batches bigint,
    total_leads_ingested bigint
) language sql stable as $$
    select
        count(*)::bigint as total_batches,
        count(*) filter (where status = 'completed')::bigint as successful_batches,
        count(*) filter (where status = 'failed')::bigint as failed_batches,
        coalesce(sum(records_inserted), 0)::bigint as total_leads_ingested
    from ingestion_log
    where started_at >= (now() - (p_days || ' days')::interval);
$$;

-- ========================================
-- Sample data for 8 capital cities
-- ========================================
insert into leads (lead_id, source, ingestion_batch_id, business_name, category, state, city, phone, email, lead_score)
values
    -- Sydney, NSW
    ('SYD-001', 'google_maps', uuid_generate_v4(), 'Sydney Harbour Plumbing', 'Plumber', 'NSW', 'Sydney', '02-9999-1234', 'info@sydneyharbourplumbing.com.au', 85),
    ('SYD-002', 'yellow_pages', uuid_generate_v4(), 'Melbourne Electrical Co', 'Electrician', 'VIC', 'Melbourne', '03-8888-5678', 'contact@melbelectrical.com.au', 72),

    -- Melbourne, VIC
    ('MEL-001', 'tradie_portal', uuid_generate_v4(), 'Melbourne Builders Group', 'Builder', 'VIC', 'Melbourne', '03-7777-1111', 'admin@melbournebuilders.com.au', 91),
    ('MEL-002', 'google_maps', uuid_generate_v4(), 'Box Hill Roofing', 'Roofing', 'VIC', 'Melbourne', '03-9999-2222', 'quotes@boxhillroofing.com.au', 68),

    -- Brisbane, QLD
    ('BNE-001', 'yellow_pages', uuid_generate_v4(), 'Brisbane Air Con Techs', 'Air Conditioning', 'QLD', 'Brisbane', '07-4444-3333', 'service@brisbaneaircon.com.au', 79),
    ('BNE-002', 'manual', uuid_generate_v4(), 'Gold Coast Landscaping', 'Landscaper', 'QLD', 'Gold Coast', '07-6666-4444', 'hello@goldcoastlandscape.com.au', 83),

    -- Perth, WA
    ('PER-001', 'google_maps', uuid_generate_v4(), 'Perth Solar Experts', 'Solar Installer', 'WA', 'Perth', '08-5555-6666', 'info@perthsolar.com.au', 88),
    ('PER-002', 'yellow_pages', uuid_generate_v4(), 'Fremantle Cabinetry', 'Cabinet Maker', 'WA', 'Fremantle', '08-8888-7777', 'quotes@fremantlecabinets.com.au', 65),

    -- Adelaide, SA
    ('ADL-001', 'tradie_portal', uuid_generate_v4(), 'Adelaide painters Pro', 'Painter', 'SA', 'Adelaide', '08-7777-8888', 'paint@adelaidepainters.com.au', 74),
    ('ADL-002', 'manual', uuid_generate_v4(), 'Barossa Valley Winery Services', 'Winery Services', 'SA', 'Barossa Valley', '08-9999-9999', 'admin@barossaservices.com.au', 61),

    -- Hobart, TAS
    ('HBA-001', 'google_maps', uuid_generate_v4(), 'Hobbit... er, Hobart Plumbing', 'Plumber', 'TAS', 'Hobart', '03-6222-1234', 'pipefixer@hobart.plumbing.com.au', 80),
    ('HBA-002', 'yellow_pages', uuid_generate_v4(), 'Tasmanian Timber Works', 'Carpenter', 'TAS', 'Launceston', '03-6333-4567', 'tim@tasmantimber.com.au', 69),

    -- Darwin, NT
    ('DRW-001', 'manual', uuid_generate_v4(), 'Darwin Air Conditioning Specialists', 'Air Conditioning', 'NT', 'Darwin', '08-8941-1234', 'cool@darwinaircon.nt.com.au', 76),
    ('DRW-002', 'tradie_portal', uuid_generate_v4(), 'Top End Electrical', 'Electrician', 'NT', 'Darwin', '08-8942-5678', 'spark@topendelectrical.com.au', 82),

    -- Canberra, ACT
    ('CBR-001', 'google_maps', uuid_generate_v4(), 'Canberra Office Fitouts', 'Office Fitout', 'ACT', 'Canberra', '02-6248-1234', 'fitout@canberraoffice.com.au', 87),
    ('CBR-002', 'government_portal', uuid_generate_v4(), 'ACT Government Contractors', 'General Contractor', 'ACT', 'Canberra', '02-6207-5678', 'bids@actcontractors.com.au', 93);

-- Verify rowcount
do $$
declare
    cnt int;
begin
    select count(*) into cnt from leads;
    raise notice 'Sample leads inserted: %', cnt;
end $$;
