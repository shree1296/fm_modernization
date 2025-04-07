{% test dynamic_expression_test(model, column_name, model_name, expression) %}
WITH failures AS (
    SELECT *
    FROM {{ model }}
    WHERE NOT ({{ expression }})
)
SELECT COUNT(*) AS failure_count
FROM failures
{% endtest %}
