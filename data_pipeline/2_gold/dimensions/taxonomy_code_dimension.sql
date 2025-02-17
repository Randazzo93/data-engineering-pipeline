create table scratch.taxonomy_code_dimension as
with staging as (
select 
distinct taxonomy_code 
from `scratch.nppes_parsed`
)

select 
*, 
case when taxonomy_code in ('2084P0800X', '2084N0400X', '2084P0802X', 
                            '390200000X', '103TC0700X', '103G00000X', 
                            '2083P0901X', '207RP1001X', '103GA0400X') then true
     else false end as can_service_dementia_patients
from staging
