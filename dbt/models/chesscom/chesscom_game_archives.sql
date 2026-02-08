{% set column_mapping = model.config.get("meta") %}
{% set ach_m %}
	to_date(split_part(archive_url, 'games/', 2) || '/01', 'yyyy/fmm/dd')
{% endset %}

with ordered as (
		select
			{{ type_mapper(column_mapping) }},
			row_number() over (
				partition by username 
				order by payload is not null, ingested_at_utc desc
			) as rn
		from {{ source("src_chesscom", "archives") }}
	),
	deduped as (
		select 
			username, ingested_dt,
			jsonb_array_elements_text(coalesce(payload->'archives', '[]'::jsonb)) as archive_url
		from ordered
		where rn = 1
	)
select
	{{ dbt_utils.generate_surrogate_key([
		'username',
		'archive_url'
	]) }} as id,
	username,
	ingested_dt,
	archive_url,
	{{ ach_m }} as archive_month,
	row_number() over (
		partition by username
		order by {{ ach_m }} desc
	) = 1 as flag_most_recent_archive
from deduped
