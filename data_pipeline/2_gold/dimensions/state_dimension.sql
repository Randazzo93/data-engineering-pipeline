CREATE
OR REPLACE TABLE scratch.state_dimension (
    state_id INT64,
    state_abbreviation STRING,
    state_name STRING
);

INSERT INTO
    scratch.state_dimension (state_id, state_abbreviation, state_name)
VALUES
    (1, 'AL', 'Alabama'),
    (2, 'AK', 'Alaska'),
    (3, 'AZ', 'Arizona'),
    (4, 'AR', 'Arkansas'),
    (5, 'CA', 'California'),
    (6, 'CO', 'Colorado'),
    (7, 'CT', 'Connecticut'),
    (8, 'DE', 'Delaware'),
    (9, 'FL', 'Florida'),
    (10, 'GA', 'Georgia'),
    (11, 'HI', 'Hawaii'),
    (12, 'ID', 'Idaho'),
    (13, 'IL', 'Illinois'),
    (14, 'IN', 'Indiana'),
    (15, 'IA', 'Iowa'),
    (16, 'KS', 'Kansas'),
    (17, 'KY', 'Kentucky'),
    (18, 'LA', 'Louisiana'),
    (19, 'ME', 'Maine'),
    (20, 'MD', 'Maryland'),
    (21, 'MA', 'Massachusetts'),
    (22, 'MI', 'Michigan'),
    (23, 'MN', 'Minnesota'),
    (24, 'MS', 'Mississippi'),
    (25, 'MO', 'Missouri'),
    (26, 'MT', 'Montana'),
    (27, 'NE', 'Nebraska'),
    (28, 'NV', 'Nevada'),
    (29, 'NH', 'New Hampshire'),
    (30, 'NJ', 'New Jersey'),
    (31, 'NM', 'New Mexico'),
    (32, 'NY', 'New York'),
    (33, 'NC', 'North Carolina'),
    (34, 'ND', 'North Dakota'),
    (35, 'OH', 'Ohio'),
    (36, 'OK', 'Oklahoma'),
    (37, 'OR', 'Oregon'),
    (38, 'PA', 'Pennsylvania'),
    (39, 'RI', 'Rhode Island'),
    (40, 'SC', 'South Carolina'),
    (41, 'SD', 'South Dakota'),
    (42, 'TN', 'Tennessee'),
    (43, 'TX', 'Texas'),
    (44, 'UT', 'Utah'),
    (45, 'VT', 'Vermont'),
    (46, 'VA', 'Virginia'),
    (47, 'WA', 'Washington'),
    (48, 'WV', 'West Virginia'),
    (49, 'WI', 'Wisconsin'),
    (50, 'WY', 'Wyoming');

create
or replace table scratch.state_dimension as
with
    staging as (
        select
            sd.*
        except
        (npi_nppes_etl_data),
        struct (
            struct (
                date_add (
                    date("2020-01-01"),
                    interval cast(floor(rand () * 365 * 5) as int64) day
                ) as etl_runtime
            ) as _last
        ) as npi_nppes_etl_data
        from
            scratch.state_dimension sd
    )
select
    s.State_ID,
    s.State_Abbreviation,
    s.State_Name,
    struct (
        struct (
            s.npi_nppes_etl_data._last.etl_runtime as etl_runtime
        ) as last_,
        struct (
            date_diff (
                current_date,
                s.npi_nppes_etl_data._last.etl_runtime,
                day
            ) as etl_runtime,
            d.day_bin as etl_runtime_bin
        ) as days_since_
    ) as npi_nppes_etl_data
from
    staging s
    left join `scratch.day_bins_dimension` d on date_diff (
        current_date,
        s.npi_nppes_etl_data._last.etl_runtime,
        day
    ) = d.day
