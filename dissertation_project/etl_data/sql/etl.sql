
--合并ab，ti--
create table original_etl_phrase_mix as
select `index`, PN, TI_phrase, AB_phrase, concat_ws('|', TI_phrase, AB_phrase) as 'TI_AB_phrase'
from original_etl_phrase;

create table original_etl_clean
as
select substring_index(PN, ';', 1),
       TI,
       substring(AB,length('NOVELTY - ')) as 'AB',
       substring_index(substring_index(substring_index(PD, '   ', 2), '   ', -1), ' ', -1) as PD_YEAR,
       TC
from original;

create table original_etl_mix
    as
select PN, CONCAT(AB,TI) as AB_TI, PD_YEAR, TC
from original_etl_clean;

-- ===============================================================================================
create table original_etl_clean
as
select substring_index(PN, ';', 1) as PN,
       TI,
       substring(AB,length('NOVELTY - ')) as 'AB',
       substring_index(substring_index(substring_index(PD, '   ', 2), '   ', -1), ' ', -1) as PD_YEAR,
       TC
from original;

create table original_etl_mix
    as
select PN, CONCAT(AB,TI) as AB_TI, PD_YEAR, TC
from origin_data_filter_clean;

create table data_year5
as
select *from original_etl_mix where PD_YEAR in ('2019','2018','2017','2016','2015')

