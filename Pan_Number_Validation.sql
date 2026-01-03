with cte as
    (select 
       (select count(*) from stg_pan_numbers_dataset) as total_processed_records,
	   count(*) filter (where status = 'Valid PAN') as total_valid_pans ,
       count(*) filter (where status = 'Invalid PAN') as total_inValid_pans
   from view_valid_invalid_pans)
select total_processed_records,total_valid_pans,total_inValid_pans,
       total_processed_records - (total_valid_pans + total_inValid_pans) as total_missing_records
from cte;



create or replace view view_valid_invalid_pans
as
with cte_cleaned_pan as
        (select distinct upper(trim(pan_number)) as pan_number
        from stg_pan_numbers_dataset 
        where pan_number is not null
        and trim(pan_number) <> ''),
	cte_valid_pans as  
        (select * 
        from cte_cleaned_pan
        where fn_check_adj_characters(pan_number) = false 
        and fn_check_sequential_characters(substring(pan_number,1,5)) = false
        and fn_check_sequential_characters(substring(pan_number,6,4)) = false
        and pan_number ~ '^[A-Z]{5}[0-9]{4}[A-Z]$')
select cln.pan_number
, case when vld.pan_number is not null 
            then 'Valid PAN'
	   else 'Invalid PAN' 
  end as status 
from cte_cleaned_pan cln
left join cte_valid_pans vld on vld.pan_number = cln.pan_number;

select * from view_valid_invalid_pans