create or replace table scratch.nppes_parsed as 
with staging as (
select 
string_field_3 as npi_number,
timestamp_seconds(cast(cast(string_field_0 as float64) / 1000 as int64)) created_at,
string_field_1 as enumeration_type,
timestamp_seconds(cast(cast(string_field_2 as float64) / 1000 as int64)) last_updated_at,
string_field_4 as addresses,
string_field_5 as practiceLocations,
string_field_6 as basic,
replace(replace(replace(string_field_7, '\u0027', '"'), 'False', 'false'),'True','true') as taxonomies,
string_field_8 as identifiers,
string_field_9 as endpoints,
string_field_11 as etl_runtime

from `personal-project-444819.scratch.nppes_extract_json_columns`
where 1=1 
      and string_field_1 != 'enumeration_type'
)

select 
npi_number,
initcap(json_extract_scalar(basic,'$.first_name')) as first_name,
initcap(json_extract_scalar(basic,'$.last_name')) as last_name,
initcap(json_extract_scalar(basic,'$.gender')) as gender,
json_extract_scalar(basic,'$.credential') as credential,

json_extract_scalar(addresses, '$[0].address_1') as address_1,
json_extract_scalar(addresses, '$[0].city') as city,
json_extract_scalar(addresses, '$[0].state') as state,
json_extract_scalar(addresses, '$[0].telephone_number') as telephone_number,

json_extract_scalar(taxonomies, '$[0].code') as taxonomy_code_1,
json_extract_scalar(taxonomies, '$[0].desc') as taxonomy_description_1,
json_extract_scalar(taxonomies, '$[0].primary') as taxonomy_primary_1,

json_extract_scalar(taxonomies, '$[1].code') as taxonomy_code_2,
json_extract_scalar(taxonomies, '$[1].desc') as taxonomy_description_2,
json_extract_scalar(taxonomies, '$[1].primary') as taxonomy_primary_2,

json_extract_scalar(taxonomies, '$[2].code') as taxonomy_code_3,
json_extract_scalar(taxonomies, '$[2].desc') as taxonomy_description_3,
json_extract_scalar(taxonomies, '$[2].primary') as taxonomy_primary_3,

json_extract_scalar(taxonomies, '$[3].code') as taxonomy_code_4,
json_extract_scalar(taxonomies, '$[3].desc') as taxonomy_description_4,
json_extract_scalar(taxonomies, '$[3].primary') as taxonomy_primary_4,

json_extract_scalar(taxonomies, '$[4].code') as taxonomy_code_5,
json_extract_scalar(taxonomies, '$[4].desc') as taxonomy_description_5,
json_extract_scalar(taxonomies, '$[4].primary') as taxonomy_primary_5,

json_extract_scalar(taxonomies, '$[5].code') as taxonomy_code_6,
json_extract_scalar(taxonomies, '$[5].desc') as taxonomy_description_6,
json_extract_scalar(taxonomies, '$[5].primary') as taxonomy_primary_6,

json_extract_scalar(taxonomies, '$[6].code') as taxonomy_code_7,
json_extract_scalar(taxonomies, '$[6].desc') as taxonomy_description_7,
json_extract_scalar(taxonomies, '$[6].primary') as taxonomy_primary_7,

json_extract_scalar(taxonomies, '$[7].code') as taxonomy_code_8,
json_extract_scalar(taxonomies, '$[7].desc') as taxonomy_description_8,
json_extract_scalar(taxonomies, '$[7].primary') as taxonomy_primary_8,

json_extract_scalar(taxonomies, '$[8].code') as taxonomy_code_9,
json_extract_scalar(taxonomies, '$[8].desc') as taxonomy_description_9,
json_extract_scalar(taxonomies, '$[8].primary') as taxonomy_primary_9,

json_extract_scalar(taxonomies, '$[9].code') as taxonomy_code_10,
json_extract_scalar(taxonomies, '$[9].desc') as taxonomy_description_10,
json_extract_scalar(taxonomies, '$[9].primary') as taxonomy_primary_10,

json_extract_scalar(taxonomies, '$[10].code') as taxonomy_code_11,
json_extract_scalar(taxonomies, '$[10].desc') as taxonomy_description_11,
json_extract_scalar(taxonomies, '$[10].primary') as taxonomy_primary_11,

json_extract_scalar(taxonomies, '$[11].code') as taxonomy_code_12,
json_extract_scalar(taxonomies, '$[11].desc') as taxonomy_description_12,
json_extract_scalar(taxonomies, '$[11].primary') as taxonomy_primary_12,

json_extract_scalar(taxonomies, '$[12].code') as taxonomy_code_13,
json_extract_scalar(taxonomies, '$[12].desc') as taxonomy_description_13,
json_extract_scalar(taxonomies, '$[12].primary') as taxonomy_primary_13,

json_extract_scalar(taxonomies, '$[13].code') as taxonomy_code_14,
json_extract_scalar(taxonomies, '$[13].desc') as taxonomy_description_14,
json_extract_scalar(taxonomies, '$[13].primary') as taxonomy_primary_14,

json_extract_scalar(taxonomies, '$[14].code') as taxonomy_code_15,
json_extract_scalar(taxonomies, '$[14].desc') as taxonomy_description_15,
json_extract_scalar(taxonomies, '$[14].primary') as taxonomy_primary_15,

case when json_extract_scalar(taxonomies, '$[0].primary') = 'true' then json_extract_scalar(taxonomies, '$[0].code')
     when json_extract_scalar(taxonomies, '$[1].primary') = 'true' then json_extract_scalar(taxonomies, '$[1].code')
     when json_extract_scalar(taxonomies, '$[2].primary') = 'true' then json_extract_scalar(taxonomies, '$[2].code')
     when json_extract_scalar(taxonomies, '$[3].primary') = 'true' then json_extract_scalar(taxonomies, '$[3].code')
     when json_extract_scalar(taxonomies, '$[4].primary') = 'true' then json_extract_scalar(taxonomies, '$[4].code')
     when json_extract_scalar(taxonomies, '$[5].primary') = 'true' then json_extract_scalar(taxonomies, '$[5].code')
     when json_extract_scalar(taxonomies, '$[6].primary') = 'true' then json_extract_scalar(taxonomies, '$[6].code')
     when json_extract_scalar(taxonomies, '$[7].primary') = 'true' then json_extract_scalar(taxonomies, '$[7].code')
     when json_extract_scalar(taxonomies, '$[8].primary') = 'true' then json_extract_scalar(taxonomies, '$[8].code')
     when json_extract_scalar(taxonomies, '$[9].primary') = 'true' then json_extract_scalar(taxonomies, '$[9].code')
     when json_extract_scalar(taxonomies, '$[10].primary') = 'true' then json_extract_scalar(taxonomies, '$[10].code')
     when json_extract_scalar(taxonomies, '$[11].primary') = 'true' then json_extract_scalar(taxonomies, '$[11].code')
     when json_extract_scalar(taxonomies, '$[12].primary') = 'true' then json_extract_scalar(taxonomies, '$[12].code')
     when json_extract_scalar(taxonomies, '$[13].primary') = 'true' then json_extract_scalar(taxonomies, '$[13].code')
     when json_extract_scalar(taxonomies, '$[14].primary') = 'true' then json_extract_scalar(taxonomies, '$[14].code')
     else null end as taxonomy_code_primary,
  
case when json_extract_scalar(taxonomies, '$[0].primary') = 'true' then json_extract_scalar(taxonomies, '$[0].desc')
     when json_extract_scalar(taxonomies, '$[1].primary') = 'true' then json_extract_scalar(taxonomies, '$[1].desc')
     when json_extract_scalar(taxonomies, '$[2].primary') = 'true' then json_extract_scalar(taxonomies, '$[2].desc')
     when json_extract_scalar(taxonomies, '$[3].primary') = 'true' then json_extract_scalar(taxonomies, '$[3].desc')
     when json_extract_scalar(taxonomies, '$[4].primary') = 'true' then json_extract_scalar(taxonomies, '$[4].desc')
     when json_extract_scalar(taxonomies, '$[5].primary') = 'true' then json_extract_scalar(taxonomies, '$[5].desc')
     when json_extract_scalar(taxonomies, '$[6].primary') = 'true' then json_extract_scalar(taxonomies, '$[6].desc')
     when json_extract_scalar(taxonomies, '$[7].primary') = 'true' then json_extract_scalar(taxonomies, '$[7].desc')
     when json_extract_scalar(taxonomies, '$[8].primary') = 'true' then json_extract_scalar(taxonomies, '$[8].desc')
     when json_extract_scalar(taxonomies, '$[9].primary') = 'true' then json_extract_scalar(taxonomies, '$[9].desc')
     when json_extract_scalar(taxonomies, '$[10].primary') = 'true' then json_extract_scalar(taxonomies, '$[10].desc')
     when json_extract_scalar(taxonomies, '$[11].primary') = 'true' then json_extract_scalar(taxonomies, '$[11].desc')
     when json_extract_scalar(taxonomies, '$[12].primary') = 'true' then json_extract_scalar(taxonomies, '$[12].desc')
     when json_extract_scalar(taxonomies, '$[13].primary') = 'true' then json_extract_scalar(taxonomies, '$[13].desc')
     when json_extract_scalar(taxonomies, '$[14].primary') = 'true' then json_extract_scalar(taxonomies, '$[14].desc')
     else null end as taxonomy_description_primary,
case when cast(taxonomies as string) like '%207RP1001X%' then true 
     else false end as is_207RP1001X_,
case when cast(taxonomies as string) like '%2084P0802X%' then true 
     else false end as is_2084P0802X_,
case when cast(taxonomies as string) like '%2084N0400X%' then true 
     else false end as is_2084N0400X_,
case when cast(taxonomies as string) like '%103G00000X%' then true 
     else false end as is_103G00000X_,
case when cast(taxonomies as string) like '%2083P0901X%' then true 
     else false end as is_2083P0901X_,
case when cast(taxonomies as string) like '%2084P0800X%' then true 
     else false end as is_2084P0800X_,
case when cast(taxonomies as string) like '%390200000X%' then true 
     else false end as is_390200000X_,
case when cast(taxonomies as string) like '%103TC0700X%' then true 
     else false end as is_103TC0700X_,
etl_runtime

from staging