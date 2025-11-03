from jinja2 import Environment, FileSystemLoader
import os

project_root = os.path.dirname(os.path.abspath(__file__))

dialects_folder = project_root
template_folder = os.path.join(project_root, "column/accepted_values/text_found_in_set_percent")

env = Environment(loader = FileSystemLoader([template_folder, dialects_folder]),
                  trim_blocks=True,
                  lstrip_blocks=True)

template = env.get_template("spark.sql.jinja2")

context = {
    # 1. Bảng nguồn
    'target_table': {
        'schema_name': 'ecommerce',
        'table_name': 'orders'
    },
    
    # 2. Metadata
    'table': {
        'filter': 'is_deleted = FALSE',
        'columns': {
            'total_price': {
                'type_snapshot': {'column_type': 'DECIMAL'},
                'sql_expression': None
            },
            'order_id': {
                'type_snapshot': {'column_type': 'VARCHAR'},
                'sql_expression': None
            },
            'created_at': {
                'type_snapshot': {'column_type': 'TIMESTAMP'},
                'sql_expression': None
            },
            'customer_region': {
                'type_snapshot': {'column_type': 'VARCHAR'},
                'sql_expression': None
            }
        }
    },
    
    # 3. Cột phân tích
    'column_name': 'total_price',
    
    # 4. Cấu hình lấy mẫu
    'error_sampling': {
        'samples_limit': 5,
        'total_samples_limit': 1000,
        'id_columns': ['order_id']
    },
    
    # 5. Nhóm dữ liệu
    'data_groupings': {
        'region': {
            'source': 'column_value',
            'column': 'customer_region'
        }
    },
    
    # 6. Time series
    'time_series': {
        'mode': 'timestamp_column',
        'timestamp_column': 'created_at',
        'time_gradient': 'day'
    },
    
    # 7. Lọc thời gian
    'time_window_filter': {
        'daily_partitioning_recent_days': 30,
        'daily_partitioning_include_today': False
    },
    
    # 8. Tham số bổ sung
    'parameters': {
        'filter': 'payment_status = "completed"',
        "expected_values": ["A", "B", "C"] 
    },
    
    # 9. Filters động
    'additional_filters': []
}

sql = template.render(**context)

print("=== Generated SQL ===\n")
print(sql)
print("\n=== End of SQL ===")