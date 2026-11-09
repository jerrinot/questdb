-- 500M probe rows, six GB of fixed-width input fits the ARM control's memory.
-- Keep keys and aggregate values identical to the wider dependent-join control.
create table p as (select cast(x*15485863%262144 as int) k, x%97-48 v from long_sequence(500000000));
create table a as (select cast(262144-x as int) k, cast(((262144-x)*73+19)%4096 as int) nk, (262144-x)%13 w from long_sequence(262144));
create table b as (select cast(x-1 as int) k, (x-1)%11 w from long_sequence(4096));
create table av as (select a.*,cast(case when k%19=0 then null when k%19=1 then '' when k%19=2 then 'é中' when k%19=3 then 'long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-long-ä-' else 'name-'||cast(k%127 as string) end as varchar) name from a);
