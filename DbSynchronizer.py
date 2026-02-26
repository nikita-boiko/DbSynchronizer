import mysql.connector

class DbSynchronizer:
    def __init__(self, source_config, target_config):
        """
        :param source_config: dict с параметрами подключения к эталонной БД (тестовой)
        :param target_config: dict с параметрами подключения к боевой БД
        """
        self.source_conn = mysql.connector.connect(**source_config)
        self.target_conn = mysql.connector.connect(**target_config)

    def get_schema(self, connection):
        """Получает структуру всех таблиц и колонок."""
        schema = {}
        cursor = connection.cursor(dictionary=True)
        
        # Получаем список таблиц
        cursor.execute("SHOW TABLES")
        tables = [list(row.values())[0] for row in cursor.fetchall()]

        for table in tables:
            cursor.execute(f"DESCRIBE `{table}`")
            columns = cursor.fetchall()
            schema[table] = {col['Field']: col for col in columns}
        
        cursor.close()
        return schema

    def generate_migration_queries(self):
        source_schema = self.get_schema(self.source_conn)
        target_schema = self.get_schema(self.target_conn)
        
        migration_sql = []
        source_cursor = self.source_conn.cursor()

        for table_name, columns in source_schema.items():
            # 1. Если таблицы на боевой нет — создаем её целиком
            if table_name not in target_schema:
                source_cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
                create_sql = source_cursor.fetchone()[1]
                migration_sql.append(f"{create_sql};")
                continue

            # 2. Если таблица есть, проверяем колонки
            for col_name, source_col in columns.items():
                target_table = target_schema[table_name]
                
                sql_col_definition = self._build_column_sql(source_col)
                
                if col_name not in target_table:
                    # Колонки нет — добавляем
                    migration_sql.append(f"ALTER TABLE `{table_name}` ADD COLUMN {sql_col_definition};")
                else:
                    # Колонка есть — сравниваем параметры (тип, null, default)
                    if self._is_column_different(source_col, target_table[col_name]):
                        migration_sql.append(f"ALTER TABLE `{table_name}` MODIFY COLUMN {sql_col_definition};")

        source_cursor.close()
        return migration_sql

    def _build_column_sql(self, col):
        """Превращает описание колонки из DESCRIBE в строку SQL."""
        name = col['Field']
        c_type = col['Type']
        null = "NULL" if col['Null'] == "YES" else "NOT NULL"
        default = f"DEFAULT '{col['Default']}'" if col['Default'] is not None else ""
        extra = col['Extra'] # например, auto_increment
        return f"`{name}` {c_type} {null} {default} {extra}".strip()

    def _is_column_different(self, col1, col2):
        """Сравнивает две колонки на предмет критических различий."""
        fields_to_compare = ['Type', 'Null', 'Default', 'Extra']
        for field in fields_to_compare:
            if col1[field] != col2[field]:
                return True
        return False

    def __del__(self):
        self.source_conn.close()
        self.target_conn.close()

# --- ПРИМЕР ИСПОЛЬЗОВАНИЯ ---

test_db_conf = {
    'host': 'localhost', 'user': 'root', 'password': '', 'database': 'test_project'
}
prod_db_conf = {
    'host': 'localhost', 'user': 'root', 'password': '', 'database': 'prod_project'
}

sync = DbSynchronizer(test_db_conf, prod_db_conf)
queries = sync.generate_migration_queries()

if not queries:
    print("Базы данных синхронизированы.")
else:
    print("-- Скрипт обновления боевой базы:\n")
    for sql in queries:
        print(sql)
