use krasavinan;

SET hive.auto.convert.join=false;
-- logs counts = 10092

-- full
select t.region as region , sum(t.men)  + sum(t.women) as counts
from (
select 
    ipregions.region, 
    if(sex="male", count(1), 0) as men, 
    if(sex="female", count(1), 0) as women 
from logs
inner join (select ip, sex from users) as sel_users
on logs.ip = sel_users.ip
inner join ipregions
on logs.ip = ipregions.ip
group by sel_users.ip, sel_users.sex, ipregions.region
having ipregions.region in ('Moscow','Krasnodar','Leningrad','Sverdlovsk','Rostov')
)t
group by t.region;

--rows = 7500 
select t.region as region , sum(t.men) + sum(t.women) as counts
from (
select 
    ipregions.region, 
    if(sex="male", count(1), 0) as men, 
    if(sex="female", count(1), 0) as women 
from logs TABLESAMPLE(7500 ROWS)
inner join (select ip, sex from users) as sel_users
on logs.ip = sel_users.ip
inner join ipregions
on logs.ip = ipregions.ip
group by sel_users.ip, sel_users.sex, ipregions.region
having ipregions.region in ('Moscow','Krasnodar','Leningrad','Sverdlovsk','Rostov')
) t
group by t.region;
    
--rows = 5000 
select t.region as region , sum(t.men) + sum(t.women) as counts
from (
select 
    ipregions.region, 
    if(sex="male", count(1), 0) as men, 
    if(sex="female", count(1), 0) as women 
from logs TABLESAMPLE(5000 ROWS)
inner join (select ip, sex from users) as sel_users
on logs.ip = sel_users.ip
inner join ipregions
on logs.ip = ipregions.ip
group by sel_users.ip, sel_users.sex, ipregions.region
having ipregions.region in ('Moscow','Krasnodar','Leningrad','Sverdlovsk','Rostov')
) t
group by t.region;

--rows = 2500 
select t.region as region , sum(t.men) + sum(t.women) as counts
from (
select 
    ipregions.region, 
    if(sex="male", count(1), 0) as men, 
    if(sex="female", count(1), 0) as women 
from logs TABLESAMPLE(2500 ROWS)
inner join (select ip, sex from users) as sel_users
on logs.ip = sel_users.ip
inner join ipregions
on logs.ip = ipregions.ip
group by sel_users.ip, sel_users.sex, ipregions.region
having ipregions.region in ('Moscow','Krasnodar','Leningrad','Sverdlovsk','Rostov')
) t
group by t.region;

--rows = 1000 
select t.region as region , sum(t.men) + sum(t.women) as counts
from (
select 
    ipregions.region, 
    if(sex="male", count(1), 0) as men, 
    if(sex="female", count(1), 0) as women 
from logs TABLESAMPLE(1000 ROWS)
inner join (select ip, sex from users) as sel_users
on logs.ip = sel_users.ip
inner join ipregions
on logs.ip = ipregions.ip
group by sel_users.ip, sel_users.sex, ipregions.region
having ipregions.region in ('Moscow','Krasnodar','Leningrad','Sverdlovsk','Rostov')
) t
group by t.region;

--rows = 100 
select t.region as region , sum(t.men) + sum(t.women) as counts
from (
select 
    ipregions.region, 
    if(sex="male", count(1), 0) as men, 
    if(sex="female", count(1), 0) as women 
from logs TABLESAMPLE(100 ROWS)
inner join (select ip, sex from users) as sel_users
on logs.ip = sel_users.ip
inner join ipregions
on logs.ip = ipregions.ip
group by sel_users.ip, sel_users.sex, ipregions.region
having ipregions.region in ('Moscow','Krasnodar','Leningrad','Sverdlovsk','Rostov')
) t
group by t.region;