SELECT * FROM weather;

--select cast(cast(Temperature['value'] as string) as float64) temp  from weather where _key like '%Vancouver#%01-10-2022#10:00%'

-- select cast(cast(Speed['value'] as string) as float64) as wind from weather where _key like '%Portland#%09-2022%'
-- order by wind desc
-- limit 1;


-- select * from weather where _key like '%SeaTac%';


-- select cast(cast(Temperature['value'] as string) as float64) temp from weather where _key like "%07-2022%" or _key like "%08-2022%"
-- order by temp desc
-- limit 1;