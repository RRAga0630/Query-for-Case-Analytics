ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD hh24:mi:ss';

with casehist1 as (
select distinct a.caseid as case_hist_id, a.oldvalue, a.newvalue, a.field, b.createddate as opened_date, a.createddate as transferred_date, b.closeddate,
case when b.closeddate is not null and a.createddate > b.closeddate then b.closeddate else a.createddate end as trans_date
from sfdc.sf_casehistory a 
left join sfdc.sf_case_reduced_column_set b on a.caseid = b.id
where a.field = 'Owner' and oldvalue not like '00%'
and a.CREATEDDATE >= TO_DATE('2022-04-01','YYYY-MM-DD')
order by a.caseid, a.createddate desc),

casehist2 as (
select distinct case_hist_id, oldvalue, newvalue, opened_date, transferred_date, closeddate, trans_date,
    SYSDATE - transferred_date as days_diff,
    transferred_date - opened_date as opened_transferred,
    row_number() over (partition by case_hist_id order by transferred_date) rn
from casehist1 
where field = 'Owner' and oldvalue not like '00%'
order by case_hist_id, transferred_date desc),

casehist3 as (
select distinct case_hist_id, oldvalue as old_caseowner, newvalue as new_caseowner, opened_date, transferred_date, opened_transferred, days_diff, closeddate, trans_date, rn,
    case when days_diff = coalesce((lag(days_diff) over (partition by case_hist_id order by transferred_date) - days_diff), days_diff) then null
    else coalesce((lag(days_diff) over (partition by case_hist_id order by transferred_date) - days_diff), days_diff) end as diff
from casehist2),

owners as (
select distinct id, name as owner_name from sfdc.vw_sf_ownerid where name is not null and id != '00G60000003NVMaEAO'),

cases_id as (
select distinct a.case_hist_id, a.old_caseowner, o1.id as old_ownerid, a.new_caseowner, o2.id as new_ownerid, 
    a.opened_date, a.transferred_date, a.closeddate, a.trans_date, a.rn, a.opened_transferred, a.days_diff, a.diff, 
    case when a.rn = 1 then a.opened_transferred else a.diff end as final_diff    
from casehist3 a 
left join owners o1 on a.old_caseowner = o1.owner_name
left join owners o2 on a.new_caseowner = o2.owner_name),

cc_grouping as (
select a.case_hist_id, a.old_caseowner, a.old_ownerid, coc1.customer_care_group as old_customer_care_group, 
    a.new_caseowner, a.new_ownerid, coc2.customer_care_group as new_customer_care_group,
    a.transferred_date, a.diff, a.final_diff, a.rn,
    coc1.customer_care_group || ' to '|| coc2.customer_care_group as transfer_hist,
    case when coc1.customer_care_group || ' to '|| coc2.customer_care_group = 'NO to NO' then 1 else 0 end as NCC_NCC,
    case when coc1.customer_care_group || ' to '|| coc2.customer_care_group = 'NO to YES' then 1 else 0 end as NCC_CC,
    case when coc1.customer_care_group || ' to '|| coc2.customer_care_group = 'YES to YES' then 1 else 0 end as CC_CC,
    case when coc1.customer_care_group || ' to '|| coc2.customer_care_group = 'YES to NO' then 1 else 0 end as CC_NCC
from cases_id a 
left join sfdc.rpt_case_owner_classification coc1 on a.old_ownerid = coc1.owner_id
left join sfdc.rpt_case_owner_classification coc2 on a.new_ownerid = coc2.owner_id
order by a.case_hist_id, a.rn),

NCC_days as (
select case_hist_id, sum(final_diff) as NCC_days
from cc_grouping where transfer_hist = 'NO to YES'
group by case_hist_id), 

case_assignment as (
select case_hist_id, sum(NCC_NCC) as NCC_NCC, sum(NCC_CC) as NCC_CC, sum(CC_CC) as CC_CC, sum(CC_NCC) as CC_NCC
from cc_grouping group by case_hist_id)

select a.*, b.NCC_days
from case_assignment a 
left join NCC_days b on a.case_hist_id = b.case_hist_id
where b.NCC_days is not null
;
