set names 'utf8';
select a.account_id,
c.main_category_id as game,
c.platform_id as platform,
login_forwarded_ip as login_ip,
device_id as login_device_id,
login_time
from login_log as a
inner join account_app as b
on a.account_id = b.account_id
inner join app c
on b.app_id = c.id
where login_time between '$from_date' and '$to_date';
