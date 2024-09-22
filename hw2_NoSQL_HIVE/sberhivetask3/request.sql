USE krasavinan;

-- var 1
-- SELECT region,
-- SUM(IF(sex="male", 1, 0)) AS male,
-- SUM(IF(sex="female", 1, 0)) AS female
-- FROM Logs, Users, IPRegions
-- WHERE Logs.ip=Users.ip AND Logs.ip = IPRegions.ip
-- GROUP BY region;


-- var 2 
-- SET hive.auto.convert.join=false;
-- SET mapreduce.job.reduces=15;

-- select ipregions.region, sum(men) as count_men, sum(women) as count_women
-- from (
--     select 
--         sel_users.ip, 
--         IF(sex="male", count(1), 0) as men, 
--         IF(sex="female", count(1), 0) as women 
--     from logs
--     inner join (select ip, sex from users) as sel_users
--     on Logs.ip = sel_users.ip
--     group by sel_users.ip, sel_users.sex) t1
-- inner join ipregions
-- on t1.ip = ipregions.ip
-- group by ipregions.region;

-- var 3
SET hive.auto.convert.join=false;
select t1.reg, sum(t1.men) as count_men, sum(t1.women) as count_women
from (
    select 
        users.ip as ip, 
        ipregions.region as reg,
        IF(sex="male", count(1), 0) as men, 
        IF(sex="female", count(1), 0) as women 
    from logs
    inner join users
    on logs.ip = users.ip
    inner join ipregions
    on users.ip = ipregions.ip
    group by users.ip, users.sex, ipregions.region
) t1
group by t1.reg;


-- select hitCount.browser, SUM(men) as c_men, SUM(women) as c_women
-- from (
--     select browser, IF(gender='male', count(1), 0) AS men, IF(gender='female', count(1), 0) AS women
--     from logs inner join (select gender, ip from users) as sel_users on logs.ip = sel_users.ip
--     group by browser, gender;
-- ) as hitCount
-- group by hitCount.browser;
