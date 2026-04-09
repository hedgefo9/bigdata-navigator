-- Mock dataset for ClickHouse (data_catalog)
-- Domain: smart workplace operations (corporate, but non-classic)
-- Includes:
--   1) 10 business tables with table/column comments
--   2) mock data inserts
--   3) metadata views matching data_scout/models.py fields

CREATE DATABASE IF NOT EXISTS data_catalog;
USE data_catalog;

DROP VIEW IF EXISTS column_metadata;
DROP VIEW IF EXISTS table_metadata;

DROP TABLE IF EXISTS vendor_quotes;
DROP TABLE IF EXISTS energy_consumption_daily;
DROP TABLE IF EXISTS predictive_risk_signals;
DROP TABLE IF EXISTS support_tickets;
DROP TABLE IF EXISTS tenant_bundle_subscriptions;
DROP TABLE IF EXISTS service_bundles;
DROP TABLE IF EXISTS tenant_accounts;
DROP TABLE IF EXISTS sensor_devices;
DROP TABLE IF EXISTS workspace_studios;
DROP TABLE IF EXISTS office_sites;

CREATE TABLE office_sites
(
    site_id UInt32 COMMENT 'Unique site identifier.',
    site_code String COMMENT 'Short code used in operational systems.',
    site_name String COMMENT 'Human-readable site name.',
    region LowCardinality(String) COMMENT 'Macro region for reporting.',
    city String COMMENT 'Site city.',
    opened_on Date COMMENT 'Date when the site became operational.',
    lease_tier LowCardinality(String) COMMENT 'Lease profile: premium/core/flex.',
    status LowCardinality(String) COMMENT 'Lifecycle state of the site.',
    gfa_sq_m UInt32 COMMENT 'Gross floor area in square meters.',
    last_audit_at DateTime COMMENT 'Timestamp of latest compliance audit.'
)
ENGINE = MergeTree
ORDER BY site_id
COMMENT 'Corporate workspace sites managed by the operator.';

CREATE TABLE workspace_studios
(
    studio_id UInt32 COMMENT 'Unique studio identifier.',
    site_id UInt32 COMMENT 'Parent site identifier.',
    studio_name String COMMENT 'Public studio name.',
    floor_no Int16 COMMENT 'Floor where the studio is located.',
    capacity UInt16 COMMENT 'Maximum seat capacity.',
    focus_profile LowCardinality(String) COMMENT 'Primary usage profile of the studio.',
    hvac_zone String COMMENT 'HVAC control zone code.',
    is_24x7 UInt8 COMMENT '1 if studio is available 24/7.',
    created_at DateTime COMMENT 'Record creation timestamp.'
)
ENGINE = MergeTree
ORDER BY (site_id, studio_id)
COMMENT 'Studios and work zones inside each office site.';

CREATE TABLE sensor_devices
(
    device_id UInt64 COMMENT 'Unique telemetry device identifier.',
    studio_id UInt32 COMMENT 'Studio where the device is installed.',
    vendor_code LowCardinality(String) COMMENT 'Vendor short code.',
    model_name String COMMENT 'Device model.',
    device_type LowCardinality(String) COMMENT 'Sensor class.',
    firmware_version String COMMENT 'Current firmware version.',
    installed_at DateTime COMMENT 'Installation timestamp.',
    health_state LowCardinality(String) COMMENT 'Current health state from monitoring.',
    battery_pct Nullable(UInt8) COMMENT 'Battery level for wireless devices.',
    last_seen_at DateTime COMMENT 'Last telemetry heartbeat timestamp.'
)
ENGINE = MergeTree
ORDER BY (studio_id, device_id)
COMMENT 'IoT device fleet used for facility telemetry.';

CREATE TABLE tenant_accounts
(
    tenant_id UInt32 COMMENT 'Unique tenant identifier.',
    tenant_code String COMMENT 'Tenant code used in billing and CRM.',
    legal_name String COMMENT 'Registered legal entity name.',
    industry_cluster LowCardinality(String) COMMENT 'Primary industry segment.',
    workforce_band LowCardinality(String) COMMENT 'Approximate workforce bracket.',
    success_manager String COMMENT 'Assigned customer success manager.',
    contract_start Date COMMENT 'Master contract start date.',
    contract_end Date COMMENT 'Master contract end date.',
    billing_currency FixedString(3) COMMENT 'Billing currency (ISO-like 3 letters).',
    account_state LowCardinality(String) COMMENT 'Current account lifecycle status.',
    created_at DateTime COMMENT 'Record creation timestamp.'
)
ENGINE = MergeTree
ORDER BY tenant_id
COMMENT 'B2B tenants using managed workplace services.';

CREATE TABLE service_bundles
(
    bundle_id UInt16 COMMENT 'Unique bundle identifier.',
    bundle_code String COMMENT 'Commercial bundle code.',
    bundle_name String COMMENT 'Commercial bundle title.',
    includes_security UInt8 COMMENT '1 if physical security coverage is included.',
    includes_it_desk UInt8 COMMENT '1 if IT support desk is included.',
    includes_wellbeing UInt8 COMMENT '1 if wellbeing services are included.',
    seats_quota UInt16 COMMENT 'Nominal seat entitlement.',
    monthly_fee Decimal(12, 2) COMMENT 'Monthly fee per bundle contract.',
    sla_hours UInt16 COMMENT 'Service-level response target in hours.',
    launched_on Date COMMENT 'Go-live date of the bundle.'
)
ENGINE = MergeTree
ORDER BY bundle_id
COMMENT 'Service bundles sold to enterprise tenants.';

CREATE TABLE tenant_bundle_subscriptions
(
    subscription_id UInt64 COMMENT 'Unique subscription contract identifier.',
    tenant_id UInt32 COMMENT 'Subscribed tenant identifier.',
    bundle_id UInt16 COMMENT 'Subscribed bundle identifier.',
    site_id UInt32 COMMENT 'Site tied to this subscription scope.',
    start_date Date COMMENT 'Subscription start date.',
    end_date Nullable(Date) COMMENT 'Optional subscription end date.',
    seat_commitment UInt16 COMMENT 'Committed number of seats.',
    renewal_mode LowCardinality(String) COMMENT 'Renewal policy mode.',
    discount_pct Decimal(5, 2) COMMENT 'Commercial discount percentage.',
    contract_owner String COMMENT 'Internal commercial owner.',
    updated_at DateTime COMMENT 'Last contract update timestamp.'
)
ENGINE = MergeTree
ORDER BY (tenant_id, subscription_id)
COMMENT 'Historical and active bundle subscriptions by tenant and site.';

CREATE TABLE support_tickets
(
    ticket_id UInt64 COMMENT 'Unique ticket identifier.',
    tenant_id UInt32 COMMENT 'Tenant that raised the issue.',
    site_id UInt32 COMMENT 'Site where issue was observed.',
    studio_id UInt32 COMMENT 'Studio where issue originated.',
    reported_channel LowCardinality(String) COMMENT 'Intake channel.',
    category LowCardinality(String) COMMENT 'Support category.',
    severity LowCardinality(String) COMMENT 'Severity level.',
    opened_at DateTime COMMENT 'Ticket creation timestamp.',
    first_response_at Nullable(DateTime) COMMENT 'First response timestamp.',
    resolved_at Nullable(DateTime) COMMENT 'Resolution timestamp.',
    assignee_pod String COMMENT 'Operational pod handling the ticket.',
    ticket_status LowCardinality(String) COMMENT 'Current ticket state.',
    csat_score Nullable(UInt8) COMMENT 'Post-resolution CSAT score (1-5).'
)
ENGINE = MergeTree
ORDER BY (opened_at, ticket_id)
COMMENT 'Operational support tickets from tenants.';

CREATE TABLE predictive_risk_signals
(
    signal_id UInt64 COMMENT 'Unique risk-signal identifier.',
    device_id UInt64 COMMENT 'Device linked to the signal.',
    signal_date Date COMMENT 'Business date of model output.',
    model_name String COMMENT 'Risk model name and version.',
    risk_score Decimal(5, 4) COMMENT 'Risk score in [0..1].',
    risk_bucket LowCardinality(String) COMMENT 'Risk bucket classification.',
    probable_issue LowCardinality(String) COMMENT 'Predicted likely issue.',
    recommended_action String COMMENT 'Suggested mitigation action.',
    needs_technician UInt8 COMMENT '1 if field technician dispatch is advised.',
    created_at DateTime COMMENT 'Signal publication timestamp.'
)
ENGINE = MergeTree
ORDER BY (signal_date, signal_id)
COMMENT 'ML-generated risk signals for proactive maintenance.';

CREATE TABLE energy_consumption_daily
(
    consumption_date Date COMMENT 'Business date of consumption.',
    site_id UInt32 COMMENT 'Site identifier.',
    studio_id UInt32 COMMENT 'Studio identifier.',
    meter_id String COMMENT 'Energy meter identifier.',
    kwh Decimal(10, 2) COMMENT 'Daily energy usage in kWh.',
    co2_kg Decimal(10, 2) COMMENT 'Estimated associated CO2 in kg.',
    peak_kw Decimal(8, 2) COMMENT 'Daily observed peak load in kW.',
    tariff_band LowCardinality(String) COMMENT 'Tariff class applied for billing.',
    anomaly_flag UInt8 COMMENT '1 if anomaly was detected.',
    ingested_at DateTime COMMENT 'Data ingestion timestamp.'
)
ENGINE = MergeTree
ORDER BY (consumption_date, site_id, studio_id)
COMMENT 'Daily energy telemetry at studio granularity.';

CREATE TABLE vendor_quotes
(
    quote_id UInt64 COMMENT 'Unique vendor quote identifier.',
    vendor_code LowCardinality(String) COMMENT 'Vendor short code.',
    site_id UInt32 COMMENT 'Site requesting the quote.',
    spare_part_sku String COMMENT 'Spare part SKU code.',
    requested_on Date COMMENT 'Date procurement requested quote.',
    quoted_on Date COMMENT 'Date vendor provided quote.',
    lead_time_days UInt16 COMMENT 'Promised lead time in days.',
    quantity UInt16 COMMENT 'Requested quantity.',
    unit_price Decimal(10, 2) COMMENT 'Quoted unit price.',
    currency FixedString(3) COMMENT 'Quote currency.',
    quote_status LowCardinality(String) COMMENT 'Commercial status of quote.',
    procurement_bot_score Decimal(5, 2) COMMENT 'Internal competitiveness score (0-100).',
    buyer_note String COMMENT 'Short buyer note.'
)
ENGINE = MergeTree
ORDER BY quote_id
COMMENT 'Procurement quotes for maintenance spare parts.';

INSERT INTO office_sites
(
    site_id, site_code, site_name, region, city, opened_on, lease_tier, status, gfa_sq_m, last_audit_at
)
VALUES
    (1, 'MSK-HQ', 'Moscow River Hub', 'EMEA', 'Moscow', '2019-04-01', 'premium', 'operational', 18200, '2026-03-31 09:00:00'),
    (2, 'DXB-01', 'Dubai Marina Node', 'MEA', 'Dubai', '2020-09-15', 'premium', 'operational', 12800, '2026-03-26 11:30:00'),
    (3, 'BER-02', 'Berlin Factory Loft', 'EMEA', 'Berlin', '2021-06-21', 'core', 'operational', 9800, '2026-03-29 10:00:00'),
    (4, 'SIN-01', 'Singapore Orchard Deck', 'APAC', 'Singapore', '2018-11-08', 'premium', 'operational', 14100, '2026-04-01 08:45:00'),
    (5, 'MAD-01', 'Madrid Retiro Labs', 'EMEA', 'Madrid', '2022-03-12', 'flex', 'stabilizing', 7600, '2026-03-22 14:05:00'),
    (6, 'MEX-01', 'Mexico City Reforma Dock', 'LATAM', 'Mexico City', '2023-01-18', 'core', 'ramp_up', 8700, '2026-03-25 16:20:00');

INSERT INTO workspace_studios
(
    studio_id, site_id, studio_name, floor_no, capacity, focus_profile, hvac_zone, is_24x7, created_at
)
SELECT
    number + 1 AS studio_id,
    (number % 6) + 1 AS site_id,
    concat(arrayElement(['Focus', 'Hybrid', 'Lab', 'Media'], (number % 4) + 1), ' Studio ', toString(number + 1)) AS studio_name,
    toInt16((number % 8) + 1) AS floor_no,
    toUInt16(12 + ((number % 6) * 8)) AS capacity,
    arrayElement(['quiet', 'hybrid', 'lab', 'broadcast'], (number % 4) + 1) AS focus_profile,
    concat('HZ-', toString((number % 12) + 1)) AS hvac_zone,
    toUInt8(if(number % 5 = 0 OR number % 4 = 2, 1, 0)) AS is_24x7,
    toDateTime('2024-01-01 09:00:00') + toIntervalDay(number * 3) AS created_at
FROM numbers(24);

INSERT INTO sensor_devices
(
    device_id, studio_id, vendor_code, model_name, device_type, firmware_version,
    installed_at, health_state, battery_pct, last_seen_at
)
SELECT
    100001 + number AS device_id,
    (number % 24) + 1 AS studio_id,
    arrayElement(['AEROSENSE', 'NEXGRID', 'OMNIGATE', 'VOLTIQ'], (number % 4) + 1) AS vendor_code,
    arrayElement(['AQ-400', 'OC-210', 'EM-900', 'AC-75'], (number % 4) + 1) AS model_name,
    device_type,
    concat('v', toString(1 + (number % 3)), '.', toString(number % 10)) AS firmware_version,
    toDateTime('2024-02-01 00:00:00') + toIntervalHour(number * 6) AS installed_at,
    multiIf(
        number % 17 = 0, 'critical',
        number % 9 = 0, 'warning',
        number % 13 = 0, 'offline',
        'healthy'
    ) AS health_state,
    if(device_type IN ('air_quality', 'occupancy'), toUInt8(40 + (number % 56)), CAST(NULL AS Nullable(UInt8))) AS battery_pct,
    now() - toIntervalMinute(number % 1800) AS last_seen_at
FROM
(
    SELECT
        number,
        arrayElement(['air_quality', 'occupancy', 'energy_meter', 'access_control'], (number % 4) + 1) AS device_type
    FROM numbers(180)
);

INSERT INTO tenant_accounts
(
    tenant_id, tenant_code, legal_name, industry_cluster, workforce_band, success_manager,
    contract_start, contract_end, billing_currency, account_state, created_at
)
VALUES
    (1, 'TN-001', 'Northstar Media Labs LLC', 'media-tech', '200-500', 'Elena Morozova', '2024-01-01', '2026-12-31', 'USD', 'active', '2023-12-12 10:00:00'),
    (2, 'TN-002', 'Blue Orbit Retail Analytics', 'retail-ai', '500-1000', 'Mark Hughes', '2023-09-15', '2026-09-14', 'EUR', 'active', '2023-08-20 11:15:00'),
    (3, 'TN-003', 'GridCraft Mobility Pte Ltd', 'mobility', '100-200', 'Aida Karim', '2024-03-01', '2027-02-28', 'SGD', 'active', '2024-02-01 09:20:00'),
    (4, 'TN-004', 'Harbor Quantum Services GmbH', 'deep-tech', '50-100', 'Nikita Belov', '2022-10-01', '2025-09-30', 'EUR', 'churn_risk', '2022-09-03 13:10:00'),
    (5, 'TN-005', 'KiteHealth Data Collective', 'health-data', '200-500', 'Elena Morozova', '2024-06-01', '2027-05-31', 'USD', 'active', '2024-05-01 12:00:00'),
    (6, 'TN-006', 'Atlas Green Construction SA', 'green-build', '1000-3000', 'Mark Hughes', '2023-01-01', '2026-12-31', 'EUR', 'active', '2022-11-21 17:40:00'),
    (7, 'TN-007', 'Pulse FinSec Operations', 'finsec', '500-1000', 'Aida Karim', '2022-04-01', '2026-03-31', 'USD', 'paused', '2022-03-16 08:25:00'),
    (8, 'TN-008', 'NeonStream Commerce Labs', 'commerce', '100-200', 'Nikita Belov', '2024-02-15', '2027-02-14', 'AED', 'active', '2024-01-05 14:00:00'),
    (9, 'TN-009', 'TerraVision Asset Intelligence', 'proptech', '200-500', 'Elena Morozova', '2023-07-01', '2026-06-30', 'EUR', 'active', '2023-05-12 10:35:00'),
    (10, 'TN-010', 'SignalForge Cloud Robotics', 'robotics', '50-100', 'Mark Hughes', '2025-01-01', '2027-12-31', 'USD', 'active', '2024-11-10 16:05:00'),
    (11, 'TN-011', 'Vertex Public Mobility Ops', 'public-mobility', '3000+', 'Aida Karim', '2023-05-01', '2028-04-30', 'USD', 'active', '2023-03-22 09:55:00'),
    (12, 'TN-012', 'Echo Adaptive Learning Group', 'edtech', '200-500', 'Nikita Belov', '2024-08-01', '2027-07-31', 'EUR', 'active', '2024-07-08 15:30:00');

INSERT INTO service_bundles
(
    bundle_id, bundle_code, bundle_name, includes_security, includes_it_desk, includes_wellbeing,
    seats_quota, monthly_fee, sla_hours, launched_on
)
VALUES
    (1, 'B-CORE', 'Core Ops', 1, 1, 0, 80, 14990.00, 8, '2022-01-01'),
    (2, 'B-FLOW', 'Flow Productivity', 1, 1, 1, 140, 23400.00, 6, '2022-09-01'),
    (3, 'B-LABX', 'Lab Extended', 1, 1, 1, 220, 38850.00, 4, '2023-02-01'),
    (4, 'B-NIGHT', '24x7 Night Ops', 1, 1, 0, 120, 27100.00, 3, '2023-07-15'),
    (5, 'B-LITE', 'Lite Satellite', 0, 1, 0, 40, 7800.00, 12, '2024-04-01');

INSERT INTO tenant_bundle_subscriptions
(
    subscription_id, tenant_id, bundle_id, site_id, start_date, end_date, seat_commitment,
    renewal_mode, discount_pct, contract_owner, updated_at
)
SELECT
    500001 + number AS subscription_id,
    (number % 12) + 1 AS tenant_id,
    (number % 5) + 1 AS bundle_id,
    ((number + intDiv(number, 3)) % 6) + 1 AS site_id,
    toDate('2024-01-01') + toIntervalDay(number * 9) AS start_date,
    if(number % 7 = 0, toDate('2025-10-01') + toIntervalDay(number), CAST(NULL AS Nullable(Date))) AS end_date,
    toUInt16(35 + (number % 10) * 15) AS seat_commitment,
    arrayElement(['auto', 'manual', 'pilot'], (number % 3) + 1) AS renewal_mode,
    toDecimal64((number % 6) * 2, 2) AS discount_pct,
    arrayElement(['Liam Costa', 'Marta Volnova', 'Owen Patel', 'Rina Chen'], (number % 4) + 1) AS contract_owner,
    now() - toIntervalDay(number % 45) AS updated_at
FROM numbers(28);

INSERT INTO support_tickets
(
    ticket_id, tenant_id, site_id, studio_id, reported_channel, category, severity,
    opened_at, first_response_at, resolved_at, assignee_pod, ticket_status, csat_score
)
SELECT
    700001 + number AS ticket_id,
    (number % 12) + 1 AS tenant_id,
    (number % 6) + 1 AS site_id,
    (number % 24) + 1 AS studio_id,
    reported_channel,
    category,
    severity,
    opened_at,
    if(
        ticket_status = 'new',
        CAST(NULL AS Nullable(DateTime)),
        opened_at + toIntervalMinute(15 + (number % 90))
    ) AS first_response_at,
    if(
        ticket_status IN ('resolved', 'closed'),
        opened_at + toIntervalHour(6 + (number % 72)),
        CAST(NULL AS Nullable(DateTime))
    ) AS resolved_at,
    arrayElement(['pod-alpha', 'pod-beta', 'pod-gamma', 'pod-delta'], (number % 4) + 1) AS assignee_pod,
    ticket_status,
    if(
        ticket_status IN ('resolved', 'closed'),
        toUInt8(3 + (number % 3)),
        CAST(NULL AS Nullable(UInt8))
    ) AS csat_score
FROM
(
    SELECT
        number,
        now() - toIntervalHour((number * 2) + 6) AS opened_at,
        arrayElement(['app', 'email', 'kiosk', 'slack'], (number % 4) + 1) AS reported_channel,
        arrayElement(['comfort', 'network', 'access', 'cleaning', 'billing'], (number % 5) + 1) AS category,
        multiIf(number % 18 = 0, 'S1', number % 7 = 0, 'S2', number % 3 = 0, 'S3', 'S4') AS severity,
        multiIf(
            number % 19 = 0, 'new',
            number % 13 = 0, 'escalated',
            number % 5 = 0, 'in_progress',
            number % 2 = 0, 'resolved',
            'closed'
        ) AS ticket_status
    FROM numbers(420)
);

INSERT INTO predictive_risk_signals
(
    signal_id, device_id, signal_date, model_name, risk_score, risk_bucket,
    probable_issue, recommended_action, needs_technician, created_at
)
SELECT
    800001 + number AS signal_id,
    100001 + (number % 180) AS device_id,
    today() - toIntervalDay(number % 45) AS signal_date,
    arrayElement(['risk-forest-v4', 'ops-xgb-v2', 'telemetry-net-v3'], (number % 3) + 1) AS model_name,
    risk_score,
    multiIf(
        risk_score >= toDecimal64(8500, 4), 'critical',
        risk_score >= toDecimal64(6500, 4), 'high',
        risk_score >= toDecimal64(4000, 4), 'medium',
        'low'
    ) AS risk_bucket,
    probable_issue,
    multiIf(
        probable_issue = 'filter_clog', 'Schedule vent cleaning in next 24h',
        probable_issue = 'network_drift', 'Run network calibration and packet-loss test',
        probable_issue = 'battery_depletion', 'Replace battery pack during next shift',
        probable_issue = 'sensor_drift', 'Recalibrate sensor against reference unit',
        'Trigger access controller integrity check'
    ) AS recommended_action,
    toUInt8(if(risk_score >= toDecimal64(6500, 4), 1, 0)) AS needs_technician,
    now() - toIntervalMinute(number * 11) AS created_at
FROM
(
    SELECT
        number,
        toDecimal64((number % 9500) + 300, 4) AS risk_score,
        arrayElement(
            ['filter_clog', 'network_drift', 'battery_depletion', 'sensor_drift', 'unauthorized_access'],
            (number % 5) + 1
        ) AS probable_issue
    FROM numbers(260)
);

INSERT INTO energy_consumption_daily
(
    consumption_date, site_id, studio_id, meter_id, kwh, co2_kg, peak_kw, tariff_band, anomaly_flag, ingested_at
)
SELECT
    consumption_date,
    site_id,
    studio_id,
    concat('MTR-', toString(studio_id)) AS meter_id,
    toDecimal64(base_kwh, 2) AS kwh,
    toDecimal64(round(base_kwh * 0.38, 2), 2) AS co2_kg,
    toDecimal64(round((base_kwh / 12.0) + ((studio_id % 5) * 0.6), 2), 2) AS peak_kw,
    arrayElement(['standard', 'green', 'time_of_use'], (site_id % 3) + 1) AS tariff_band,
    toUInt8(if((studio_id % 11 = 0) AND (day_offset % 13 = 0), 1, 0)) AS anomaly_flag,
    toDateTime(consumption_date) + toIntervalHour(3) AS ingested_at
FROM
(
    SELECT
        number,
        toUInt32((number % 24) + 1) AS studio_id,
        toUInt32(((number % 24) % 6) + 1) AS site_id,
        intDiv(number, 24) AS day_offset,
        today() - toIntervalDay(intDiv(number, 24)) AS consumption_date,
        42 + ((number % 24) % 7) * 6 + (intDiv(number, 24) % 5) * 3 + if((number % 24) % 4 = 0, 12, 0) AS base_kwh
    FROM numbers(1440)
);

INSERT INTO vendor_quotes
(
    quote_id, vendor_code, site_id, spare_part_sku, requested_on, quoted_on, lead_time_days,
    quantity, unit_price, currency, quote_status, procurement_bot_score, buyer_note
)
SELECT
    900001 + number AS quote_id,
    arrayElement(['VEND-AIR', 'VEND-NET', 'VEND-CTRL', 'VEND-ENG', 'VEND-OPS', 'VEND-GEN'], (number % 6) + 1) AS vendor_code,
    (number % 6) + 1 AS site_id,
    concat('SP-', toString((number % 45) + 100)) AS spare_part_sku,
    requested_on,
    requested_on + toIntervalDay((number % 6) + 1) AS quoted_on,
    toUInt16(2 + (number % 28)) AS lead_time_days,
    toUInt16(1 + (number % 20)) AS quantity,
    toDecimal64(40 + ((number % 17) * 8) + ((number % 6) * 2), 2) AS unit_price,
    arrayElement(['USD', 'EUR', 'AED'], (number % 3) + 1) AS currency,
    multiIf(
        number % 11 = 0, 'expired',
        number % 7 = 0, 'rejected',
        number % 5 = 0, 'accepted',
        number % 3 = 0, 'submitted',
        'draft'
    ) AS quote_status,
    toDecimal64(58 + (number % 39), 2) AS procurement_bot_score,
    arrayElement(
        [
            'Prefer consolidated shipment.',
            'Need compliance certificate copy.',
            'Evaluate alternative with faster lead time.',
            'Awaiting legal addendum on SLA.',
            'Price acceptable if prepaid terms approved.'
        ],
        (number % 5) + 1
    ) AS buyer_note
FROM
(
    SELECT
        number,
        today() - toIntervalDay((number % 90) + 10) AS requested_on
    FROM numbers(120)
);

CREATE VIEW table_metadata AS
SELECT
    name AS table_name,
    comment AS table_comment
FROM system.tables
WHERE database = 'data_catalog'
  AND engine != 'View'
ORDER BY table_name;

CREATE VIEW column_metadata AS
SELECT
    c.table AS table_name,
    c.name AS column_name,
    c.type AS data_type,
    c.comment AS column_comment,
    toBool(NOT startsWith(c.type, 'Nullable(')) AS is_not_null
FROM system.columns AS c
INNER JOIN system.tables AS t
    ON t.database = c.database
   AND t.name = c.table
WHERE c.database = 'data_catalog'
  AND t.engine != 'View'
ORDER BY table_name, c.position;

-- quick checks:
-- SELECT * FROM table_metadata;
-- SELECT * FROM column_metadata LIMIT 50;
