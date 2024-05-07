/*
1.
Query lấy số perfromance metrics như Gsv_exclude_vat, Stp, Rev theo các dimension hiện có như: complete_date, province, service, stop_partner/ partner
- Service ở đây chỉ chia theo Bike và Truck

2.
Để điều chỉnh thời gian query va mức độ chi tiết của dữ liệu cần điều chỉnh giá trị cho 3 biến sau:
    1. global_time_view: mức độ chi tiết của dữ liệu theo thứ tự giảm dần sau year/quarter/month/week/day
    2. global_start_date: Thời gian bắt đầu
    3. global_end_date: Thời gian kết thúc
*/

declare global_time_view string default 'day';
declare global_start_date date default date({start_date},'Asia/Saigon');
declare global_end_date date default CURRENT_DATE('Asia/Saigon');

create temp function date_trunc_mock(x date,timeview STRING)
    as (
        (select case
                    when timeview = 'year'   then date_trunc(x,year)
                    when timeview = 'quarter'   then date_trunc(x,quarter)
                    when timeview = 'month' then date_trunc(x,month)
                    when timeview = 'week' then date_trunc(x,week(monday))
                else  date_trunc(x,day)
            end
        )
    );

with ka_account as (
    select ka.team
        , ka.partner
        , min(ka.start_time) as start_date
        , max(ka.end_time) as end_date
    from ahamove_archive.ka_account ka
    where true
        and ka.partner not in ('ghn','ghnlastmile','haravan')
    group by 1,2
),
    raw_data as (
        select
            date_trunc_mock(f.complete_date, global_time_view) as complete_date
            , lower(f.province) province
            , case when f.partner = 'ka_warehouse' then 'warehouse'
                    when regexp_contains(f.service_id, 'BULKY') then 'bulky'
                    when regexp_contains(f.service_id, 'VAN|TRUCK|TRICYCLE') then 'truck'
                    else 'bike'
                end as service_type
            , case when f.partner in ('ghn','ghn_lastmile') then 'ghn'
                    when ka.team is not null and f.stop_partner <> 'NULL'
                        or ka_bd.name is not null then 'ka_bd_ops'
                    else 'sme'
                end as customer_type
            , case when f.partner in ('ghn','ghnlastmile') then 'GHN'
                    when ka_bd.name is not null then ka_bd.name
                    when f.stop_partner = 'NULL' then (case 
                                                        when f.partner in ('ka_warehouse','ahamove') then COALESCE(IF(hc.main_id is null, if(hc.main_id = '',hc.id,hc.main_id),hc.main_id), hc.id)
                                                            else f.partner
                                                        end )
                    else f.stop_partner
                end as partner
            , case when regexp_contains(f.service_id,'TRUCK|VAN|TRICYCLE') then 'truck'
                    when f.province not in ('HAN','SGN') then 'expansion'
                    else 'core'
                end as project
            , sum(f.gsv_excluded_vat)                         as gsv_excluded_vat
            , sum(f.stoppoint)                                as stoppoint
            , sum(f.aha_revenue)                              as aha_revenue
        from ahamove_archive_fa.fct_fa_pnl_ver2 f
            left join ka_account ka
                on (case
                        when f.stop_partner = 'NULL' then f.partner
                        else f.stop_partner
                    end ) = ka.partner
                and f.complete_date between ka.start_date and ka.end_date
            left join ahamove_archive.hubspot_contact hc
                on f.user_id = hc.id
            left join ahamove_archive_bd.bdsale_ka ka_bd
                on COALESCE(IF(hc.main_id is null, if(hc.main_id = '',hc.id,hc.main_id),hc.main_id), hc.id) = ka_bd.main_id
        where true
            and f.complete_date between global_start_date and date_sub(global_end_date, interval 1 day)
        group by 1,2,3,4,5,6
    )
select *
from raw_data
order by 1