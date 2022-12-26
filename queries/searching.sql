-- phrase search
explain analyse
with matched_keywrods as (
    select keyword, keyword_tsv, volume, ts_rank(keyword_tsv, query) as keyword_rank
    from adwords_en_us, to_tsquery('law<->apartment') as query
    where keyword_tsv @@ query
    and spell_type is null and volume is not null
)
select keyword, volume
from matched_keywrods
order by keyword_rank desc , volume desc
limit 500;


-- broad search
explain analyse
with matched_keywrods as (
    select keyword, keyword_tsv, volume, ts_rank(keyword_tsv, query) as keyword_rank
    from adwords_en_us, to_tsquery('law<->apartment') as query
    where keyword_tsv @@ query
    and spell_type is null and volume is not null
)
select keyword, volume
from matched_keywrods
order by keyword_rank desc , volume desc
limit 500;


-- Single word
explain analyse
with matched_keywrods as (
    select keyword, keyword_tsv, volume, ts_rank(keyword_tsv, query) as keyword_rank
    from adwords_en_us, to_tsquery('hotel') as query
    where keyword_tsv @@ query
    and spell_type is null and volume is not null
)
select *
from matched_keywrods
order by keyword_rank desc , volume desc
limit 500;