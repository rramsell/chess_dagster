{% macro default__generate_alias_name(custom_alias_name=none, node=none) -%}
  {%- if custom_alias_name -%}
    {{ return(custom_alias_name | trim) }}
  {%- endif -%}

  {% set meta = {} %}
  {% if node is mapping %}
    {% set _m = node.get('meta', {}) %}
    {% if _m is mapping %}
      {% do meta.update(_m) %}
    {% endif %}
    {% set _cfg = node.get('config', {}) %}
    {% if _cfg is mapping %}
      {% set _cm = _cfg.get('meta', {}) %}
      {% if _cm is mapping %}
        {% do meta.update(_cm) %}
      {% endif %}
    {% endif %}
    {% set _name = node.get('name') %}
  {% else %}
    {% if node.meta is not none %}
      {% do meta.update(node.meta) %}
    {% endif %}
    {% if node.config is not none and node.config.meta is not none %}
      {% do meta.update(node.config.meta) %}
    {% endif %}
    {% set _name = node.name %}
  {% endif %}

  {% set alias = meta.get('alias') %}
  {% if alias %}
    {{ return(alias) }}
  {% endif %}

  {{ return(_name) }}
{%- endmacro %}

{% macro generate_schema_name(custom_schema_name, node) %}
  {% if custom_schema_name is not none %}
    {{ return(custom_schema_name) }}
  {% endif %}

  {% if node is mapping %}
    {% set fqn = node.get('fqn', []) %}
  {% else %}
    {% set fqn = node.fqn %}
  {% endif %}

  {% if (fqn | length) > 2 %}
    {{ return(fqn[1]) }}
  {% endif %}

  {{ return(target.schema) }}
{% endmacro %}
