create table scratch.state_npi_facts as
with
    staging as (
        select
            sa.state_id,
            s.state,
            --city,
            count(distinct s.npi_number) as npi_count,
            count(
                distinct (
                    case
                        when s.is_207RP1001X_ = true
                        or s.is_2084P0802X_ = true
                        or s.is_2084N0400X_ = true
                        or s.is_103G00000X_ = true
                        or s.is_2083P0901X_ = true
                        or s.is_2084P0800X_ = true
                        or s.is_390200000X_ = true
                        or s.is_103TC0700X_ = true then npi_number
                    end
                )
            ) npi_can_service_dementia_patients
        from
            `personal-project-444819.scratch.nppes_parsed` s
            left join `personal-project-444819.scratch.state_dimension` sa on s.state = sa.State_Abbreviation
        where
            state not in ('BELLA VISTA', 'DC', 'SK', 'SINDH', 'ONTARIO')
        group by
            1,
            2
    )
select
    *,
    right (
        left (replace (cast(rand () as string), '0.', ''), 4),
        1
    ) as npi_remo_engaged,
from
    staging
