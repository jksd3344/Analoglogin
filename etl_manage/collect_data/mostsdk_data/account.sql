set names 'utf8';
select a.account_id,
d.main_category_id as game_id,
d.platform_id as platform_id,
d.channel_id as channel_id,
first_login_ip,
first_login_device_id,
first_login_time,
case 
when e.account_id is null then 0
else 1 end as type,
b.channel_id as sdk_source
from account_app as a 
inner join account as b
on a.account_id = b.id
inner join app d
on a.app_id = d.id
left outer join inside_account e
on a.account_id = e.account_id
and a.app_id = e.app_id
where first_login_time between '$from_date' and '$to_date'
