from jinja2 import Environment, BaseLoader

# Template đầy đủ với tất cả các macro
template_str = """
{% set dialect_settings = {
    "quote_begin": "`",
    "quote_end": "`",
    "quote_escape": "``"
    }
%}

{% set macro_schema_name = target_table.schema_name -%}
{% set macro_table_name = target_table.table_name -%}
{% set calculated_column_expression = table.columns[column_name].sql_expression | default('', true) -%}

{%- macro quote_identifier(name) -%}
    {{- dialect_settings.quote_begin}}{{ name | replace(dialect_settings.quote_end, dialect_settings.quote_escape)}}{{dialect_settings.quote_end -}}
{%- endmacro -%}

{%- macro render_target_table() -%}
    {{ quote_identifier(macro_schema_name) }}.{{quote_identifier(macro_table_name)}}
{%- endmacro -%}

{%- macro render_target_column(table_alias_prefix) -%}
    {%- if calculated_column_expression != '' -%}
        ({{ calculated_column_expression 
        | replace('{column}', table_alias_prefix ~ '.' ~ quote_identifier(column_name)) 
        | replace('{table}', render_target_table()) 
        | replace('{alias}', table_alias_prefix) }})
    {%- else -%}
        {{ table_alias_prefix }}.{{ quote_identifier(column_name) }}
    {%- endif -%}
{%- endmacro %}

Result: {{ render_target_column(table_alias) }}
"""

env = Environment(loader=BaseLoader())

# print("=" * 80)
# print("VÍ DỤ 1: CỘT THÔNG THƯỜNG (không có calculated expression)")
# print("=" * 80)

# # Data cho ví dụ 1
# data1 = {
#     'target_table': {
#         'schema_name': 'sales',
#         'table_name': 'employees'
#     },
#     'table': {
#         'columns': {
#             'salary': {
#                 'sql_expression': ''  # Không có expression
#             }
#         }
#     },
#     'column_name': 'salary',
#     'table_alias': 'emp'
# }

# template = env.from_string(template_str)
# result = template.render(**data1)
# print(f"""
# Input Data:
#   - Schema: {data1['target_table']['schema_name']}
#   - Table: {data1['target_table']['table_name']}
#   - Column: {data1['column_name']}
#   - Alias: {data1['table_alias']}   
#   - SQL Expression: {data1['table']['columns']['salary']['sql_expression']}

# Các bước xử lý:
#   1. macro_schema_name = 'sales'
#   2. macro_table_name = 'employees'
#   3. calculated_column_expression = '' (empty)
#   4. Vì calculated_column_expression == '', nên dùng nhánh else
#   5. Kết quả: table_alias_prefix + '.' + quote_identifier(column_name)
#      = 'emp' + '.' + '`salary`'

# Output:
# {result.split('Result:')[1].strip()}
# # """)

# print("\n" + "=" * 80)
# print("VÍ DỤ 2: CỘT TÍNH TOÁN ĐƠN GIẢN")
# print("=" * 80)

# data2 = {
#     'target_table': {
#         'schema_name': 'sales',
#         'table_name': 'products'
#     },
#     'table': {
#         'columns': {
#             'price': {
#                 'sql_expression': '{column} * 1.1'  # Tăng giá 10%
#             }
#         }
#     },
#     'column_name': 'price',
#     'table_alias': 'p'
# }

# template = env.from_string(template_str)
# result = template.render(**data2)
# print(f"""
# Input Data:
#   - Schema: {data2['target_table']['schema_name']}
#   - Table: {data2['target_table']['table_name']}
#   - Column: {data2['column_name']}
#   - Alias: {data2['table_alias']}
#   - SQL Expression: '{data2['table']['columns']['price']['sql_expression']}'

# Các bước xử lý:
#   1. macro_schema_name = 'sales'
#   2. macro_table_name = 'products'
#   3. calculated_column_expression = '{{column}} * 1.1'
#   4. Vì calculated_column_expression != '', nên xử lý replace
#   5. Replace {{column}} với: p.`price`
#      Expression: 'p.`price` * 1.1'
#   6. Bọc trong dấu ngoặc đơn: (p.`price` * 1.1)

# Output:
# {result.split('Result:')[1].strip()}
# """)

print("\n" + "=" * 80)
print("VÍ DỤ 3: CỘT TÍNH TOÁN VỚI {{table}} PLACEHOLDER")
print("=" * 80)

data3 = {
    'target_table': {
        'schema_name': 'crm',
        'table_name': 'users'
    },
    'table': {
        'columns': {
            'user_id': {
                'sql_expression': '(SELECT COUNT(*) FROM {table} sub WHERE sub.user_id = {column})'
            }
        }
    },
    'column_name': 'user_id',
    'table_alias': 'u'
}

template = env.from_string(template_str)
result = template.render(**data3)
print(f"""
Input Data:
  - Schema: {data3['target_table']['schema_name']}
  - Table: {data3['target_table']['table_name']}
  - Column: {data3['column_name']}
  - Alias: {data3['table_alias']}
  - SQL Expression: '(SELECT COUNT(*) FROM {{table}} sub WHERE sub.user_id = {{column}})'

Các bước xử lý:
  1. macro_schema_name = 'crm'
  2. macro_table_name = 'users'
  3. calculated_column_expression = '(SELECT COUNT(*) FROM {{table}} sub WHERE sub.user_id = {{column}})'
  4. Replace {{column}} với: u.`user_id`
  5. Replace {{table}} với: `crm`.`users` (từ render_target_table())
    6. Kết quả sau replace: '(SELECT COUNT(*) FROM `crm`.`users` sub WHERE sub.user_id = u.`user_id`)'

Output:
{result.split('Result:')[1].strip()}
""")

# print("\n" + "=" * 80)
# print("VÍ DỤ 4: CỘT TÍNH TOÁN VỚI {{alias}} PLACEHOLDER")
# print("=" * 80)

# data4 = {
#     'target_table': {
#         'schema_name': 'finance',
#         'table_name': 'invoices'
#     },
#     'table': {
#         'columns': {
#             'total': {
#                 'sql_expression': '{alias}.`quantity` * {alias}.`unit_price` * (1 - {alias}.`discount`)'
#             }
#         }
#     },
#     'column_name': 'total',
#     'table_alias': 'inv'
# }

# template = env.from_string(template_str)
# result = template.render(**data4)
# print(f"""
# Input Data:
#   - Schema: {data4['target_table']['schema_name']}
#   - Table: {data4['target_table']['table_name']}
#   - Column: {data4['column_name']}
#   - Alias: {data4['table_alias']}
#   - SQL Expression: '{{alias}}.`quantity` * {{alias}}.`unit_price` * (1 - {{alias}}.`discount`)'

# Các bước xử lý:
#   1. macro_schema_name = 'finance'
#   2. macro_table_name = 'invoices'
#   3. calculated_column_expression = '{{alias}}.`quantity` * {{alias}}.`unit_price` * (1 - {{alias}}.`discount`)'
#   4. Replace {{column}} với: inv.`total` (không dùng trong expression này)
#   5. Replace {{alias}} với: inv
#   6. Kết quả: 'inv.`quantity` * inv.`unit_price` * (1 - inv.`discount`)'

# Output:
# {result.split('Result:')[1].strip()}
# """)

# print("\n" + "=" * 80)
# print("VÍ DỤ 5: CASE PHỨC TẠP VỚI TẤT CẢ PLACEHOLDERS")
# print("=" * 80)

# data5 = {
#     'target_table': {
#         'schema_name': 'sales',
#         'table_name': 'orders'
#     },
#     'table': {
#         'columns': {
#             'final_amount': {
#                 'sql_expression': """CASE 
#     WHEN {column} > 1000 THEN {column} * 0.9
#     WHEN {alias}.status = 'VIP' THEN {column} * 0.85
#     WHEN EXISTS (SELECT 1 FROM {table} WHERE id = {alias}.id) THEN {column} * 0.95
#     ELSE {column}
# END"""
#             }
#         }
#     },
#     'column_name': 'final_amount',
#     'table_alias': 'ord'
# }

# template = env.from_string(template_str)
# result = template.render(**data5)
# print(f"""
# Input Data:
#   - Schema: {data5['target_table']['schema_name']}
#   - Table: {data5['target_table']['table_name']}
#   - Column: {data5['column_name']}
#   - Alias: {data5['table_alias']}
#   - SQL Expression: CASE statement phức tạp

# Các bước xử lý:
#   1. Replace {{column}} → ord.`final_amount`
#   2. Replace {{alias}} → ord
#   3. Replace {{table}} → `sales`.`orders`
  
# Output (được format lại cho dễ đọc):
# {result.split('Result:')[1].strip()}
# """)

# print("\n" + "=" * 80)
# print("VÍ DỤ 6: TÊN CỘT CÓ DẤU BACKTICK (cần escape)")
# print("=" * 80)

# data6 = {
#     'target_table': {
#         'schema_name': 'test',
#         'table_name': 'special_table'
#     },
#     'table': {
#         'columns': {
#             'column`with`backticks': {
#                 'sql_expression': ''
#             }
#         }
#     },
#     'column_name': 'column`with`backticks',
#     'table_alias': 't'
# }

# template = env.from_string(template_str)
# result = template.render(**data6)
# print(f"""
# Input Data:
#   - Schema: {data6['target_table']['schema_name']}
#   - Table: {data6['target_table']['table_name']}
#   - Column: {data6['column_name']} (có chứa dấu `)
#   - Alias: {data6['table_alias']}

# Các bước xử lý:
#   1. quote_identifier() sẽ escape dấu ` thành ``
#   2. Tên cột sau khi quote: `column``with``backticks`
#   3. Kết quả: t.`column``with``backticks`

# Output:
# {result.split('Result:')[1].strip()}
# """)

# print("\n" + "=" * 80)
# print("TỔNG KẾT CÁC PLACEHOLDER")
# print("=" * 80)
# print("""
# 1. {{column}} - Được thay bằng: table_alias.`column_name`
#    Ví dụ: {{column}} → emp.`salary`

# 2. {{table}} - Được thay bằng: `schema_name`.`table_name`
#    Ví dụ: {{table}} → `sales`.`employees`

# 3. {{alias}} - Được thay bằng: table_alias_prefix
#    Ví dụ: {{alias}} → emp

# Thứ tự thực hiện replace: {{column}} → {{table}} → {{alias}}
# """)