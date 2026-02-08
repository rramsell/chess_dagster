{% macro parse_datetime(timestamp_field) %}
	case
		when {{ timestamp_field }} is null or {{ timestamp_field }} = '' then null
		when {{ timestamp_field }} ~ '^[0-9]+$' then to_timestamp(({{ timestamp_field }})::double precision)
		else ({{ timestamp_field }})::timestamp
	end
{% endmacro %}

{% macro parse_date(date_field) %}
	case
		when {{ date_field }} is null or {{ date_field }} = '' then null
		when {{ date_field }} ~ '^[0-9]+$' then to_timestamp(({{ date_field }})::double precision)::date
		else ({{ date_field }})::date
	end
{% endmacro %}

{% macro if(condition, if_true, if_false) %}
	case when {{ condition }} then {{ if_true }} else {{ if_false }} end
{% endmacro %}

{% macro type_mapper(column_mapping) %}
	{% if column_mapping is mapping and column_mapping.get("column_mapping") is not none %}
		{% set column_mapping = column_mapping.get("column_mapping") %}
	{% endif %}

	{% if column_mapping is none %}
		{{ exceptions.raise_compiler_error("type_mapper() received null column_mapping") }}
	{% endif %}

	{% set rendered = [] %}

	{% for cast_type, mappings in column_mapping.items() %}

		{% if mappings is mapping %}
			{% for expr, alias in mappings.items() %}
				{% set e = expr | trim %}
				{% set t = cast_type | trim %}
				{% do rendered.append("(" ~ e ~ ")::" ~ t ~ " as " ~ alias) %}
			{% endfor %}
		{% else %}
			{{ exceptions.raise_compiler_error(
				"type_mapper() expects dict mappings under each type key. Got: " ~ (mappings | string)
			) }}
		{% endif %}

	{% endfor %}

	{% for col in rendered %}
		{{ col }}{{ "," if not loop.last else "" }}
	{% endfor %}
{% endmacro %}

{% macro meta_columns(column_mapping, exclude=[]) %}
	{% if column_mapping is mapping and column_mapping.get("column_mapping") is not none %}
		{% set column_mapping = column_mapping.get("column_mapping") %}
	{% endif %}

	{% if column_mapping is none %}
		{{ exceptions.raise_compiler_error("meta_columns() received null column_mapping") }}
	{% endif %}

	{% if exclude is none %}
		{% set exclude = [] %}
	{% endif %}

	{% if exclude is string %}
		{{ exceptions.raise_compiler_error("meta_columns() exclude must be a list, got string") }}
	{% endif %}

	{% set cols = [] %}

	{% for cast_type, mappings in column_mapping.items() %}

		{% if mappings is mapping %}
			{% for expr, alias in mappings.items() %}
				{% do cols.append(alias | trim) %}
			{% endfor %}
		{% else %}
			{{ exceptions.raise_compiler_error(
				"meta_columns() expects dict mappings under each type key. Got: " ~ (mappings | string)
			) }}
		{% endif %}

	{% endfor %}

	{% if exclude | length > 0 %}
		{% set exclusions = [] %}
		{% for col in exclude %}
			{% do exclusions.append(col | trim) %}
		{% endfor %}

		{% set filtered = [] %}
		{% for col in cols %}
			{% if col not in exclusions %}
				{% do filtered.append(col) %}
			{% endif %}
		{% endfor %}
		{% set cols = filtered %}
	{% endif %}

	{{ return(cols) }}
{% endmacro %}

{% macro greatest_or_least(logical_comparison, fields=[]) -%}
	-- Only accepts greatest or least
	{% set operation = (logical_comparison or '') | lower %}
	{% if operation not in ['greatest', 'least'] %}
		{{ exceptions.raise_compiler_error(
			"First arg must be 'greatest' or 'least', not: " ~ logical_comparison ~ ")"
		) }}
	{% endif %}

	{% if fields is none or (fields | length) == 0 %}
		{{ exceptions.raise_compiler_error("fields list can't be empty") }}
	{% endif %}

	{% set agg = 'max' if operation == 'greatest' else 'min' %}

	{% set values_rows = [] %}
	{% for f in fields %}
		{% if f is not none and (f | trim) != '' %}
			{% do values_rows.append("(" ~ f ~ ")") %}
		{% endif %}
	{% endfor %}

	{% if (values_rows | length) == 0 %}
		null
	{% else %}
		(
			select {{ agg }}(v)
			from (values
				{{ values_rows | join(",\n\t\t\t\t") }}
			) as t(v)
			where v is not null
		)
	{% endif %}
{%- endmacro %}
