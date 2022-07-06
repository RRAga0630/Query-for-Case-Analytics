with case_history as 
 ( select case_hist_id, oldvalue, newvalue, field, transferred_date from
   (select ch.caseid as case_hist_id, ch.oldvalue, ch.newvalue, ch.field, ch.createddate as transferred_date
           , row_number() over (partition by ch.caseid order by ch.createddate desc) rn
      from sfdc.sf_casehistory ch 
     where ch.CREATEDDATE >= TO_DATE('2022-01-01','YYYY-MM-DD')
       and ch.field='Owner'
       and ch.newvalue not like '00%'
   )
   where rn = 1
 )
select case_.id 
, account.accountnumber, account.ip_company_number__c as company_number_ip, account.entity_level_id__c as entity_level_id
, case when rt.name = 'Service Account' then account.accountnumber
       when rt.name = 'Service Account IP' then account.ip_company_number__c
       when rt.name = 'Global Ultimate Parent Account' then account.entity_level_id__c
       when rt.name = 'Regional Sales Account' then account.entity_level_id__c
       when rt.name = 'Subsidiary' then account.entity_level_id__c
       else case_.company_number__c end as company_number_rt
, case_.PARENTID
, case_.ownerid
, case_.company_number__c
, case when trim(case_.company_number__c) is null 
        and trim( case when rt.name = 'Service Account' then account.accountnumber
                       when rt.name = 'Service Account IP' then account.ip_company_number__c
                       when rt.name = 'Global Ultimate Parent Account' then account.entity_level_id__c
                       when rt.name = 'Regional Sales Account' then account.entity_level_id__c
                       when rt.name = 'Subsidiary' then account.entity_level_id__c
                       else case_.company_number__c end) is not null then 
             case when rt.name = 'Service Account' then account.accountnumber
                  when rt.name = 'Service Account IP' then account.ip_company_number__c
                  when rt.name = 'Global Ultimate Parent Account' then account.entity_level_id__c
                  when rt.name = 'Regional Sales Account' then account.entity_level_id__c
                  when rt.name = 'Subsidiary' then account.entity_level_id__c
                  else case_.company_number__c end
else case_.company_number__c end as company_number_final
, case_.CASENUMBER
, case_.CONTACTID
, case_.ACCOUNTID
, case_.TYPE
, case_.STATUS
, case_.REASON
, case_.ORIGIN 
, case_.SUBJECT
, case_.PRIORITY
, case_.ISCLOSED
, case_.CLOSEDDATE
, case_.ISESCALATED
, case_.CREATEDDATE
, case_.LASTMODIFIEDBYID
, case_.LASTMODIFIEDDATE
, case_.PRODUCT__C
, case_.INCIDENT_DATE_TIME__C
, case_.CASE_CLOSURE_TYPE__C
, case_.SERVICE__C
, case_.ACTSETUPDAYSOPEN__C
, case_.RESOLVED_TYPE__C
, case_.REGION__C
, case_.CS_BUCKET__C
, case_.DAYS_OPEN__C
, case_.SERVICE_ISSUES_2__C
, case_.SERVICE_CATEGORY__C
, case_.NATURE_OF_CHANGE__C
, case_.NUMBER_OF_TRANSACTIONS_COMPLET
, case_.CASE_RELATIONS_TYPE__C
, case_.CUSTOMER_TYPE__C
, case_.REP_CODE__C
, case_.ISTRUECLOSED__C
, case_.COMPANY__C
, case_.OWNER__C
, case_.COUNTRY__C
, case_.DURATION_IN_MINUTES__C
, case_.TASK_COMPLETED_TIME_HOURS__C
, case_.AGE_OF_CLOSED_CASE_HOURS__C
, case_.ACCOUNT_NUMBER__C
, case_.BUSINESS_UNIT__C
, case_.CATEGORY_OF_ISSUE__C
, case_.ACTION_TO_RESOLVE__C
, case_.REQUEST_TYPE__C
, case_.ISSUE_RESOLVED__C
, case_.CASE_ORIGIN_TYPE__C
, case_.TYPEOFSERVICE__C
, case_.DATE_AND_TIME_REQUESTED__C
, case_.working_agent_bco__c
, case_.working_agent_cs__c
, case_.working_agent__c 
, o.name as owner_name
, case_history.case_hist_id, case_history.oldvalue, case_history.newvalue, case_history.field, case_history.transferred_date
, case_.RECORDTYPEID
, rt.name as case_record_type, rt.developername, rt.description record_type_description
from sfdc.sf_case_reduced_column_set case_
left outer join sfdc.sf_account_reduced_column_set account on account.id = case_.accountid
left outer join sfdc.vw_sf_ownerid o on case_.ownerid = o.id
left outer join case_history on case_history.case_hist_id = case_.id
left outer join sfdc.sf_recordtype rt on rt.id = case_.recordtypeid
where case_.createddate >= TO_DATE('2022-01-01','YYYY-MM-DD')
;
