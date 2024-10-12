import psycopg2
import configparser
from psycopg2 import sql
from prettytable import PrettyTable


def config(filename='database.ini', section='postgresql'):
# Параметры подключения к серверу из файла database.ini
    parser = configparser.ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            if param[1] != '':
                db[param[0]] = param[1]
            else:
                raise Exception(f'Параметр {param[0]} не указан в файле {filename}')
    else:
        raise Exception('Раздел {0} не найден в файле {1}'.format(section, filename))
    return db

def server_test(params):
# Проверка подключения к серверу
    try:
        conn = psycopg2.connect(user=params['user'], password=params['password'], host=params['host'], port=params['port'])
    except:
        raise Exception(f'Нет подключения к серверу. Проверьте параметры.')

def db_test(params):
# Проверка базы данных
    database = params['database'] 
    with psycopg2.connect(user=params['user'], password=params['password']) as conn:
        with conn.cursor() as cur:
            sql_str = sql.SQL("SELECT 1 FROM pg_database WHERE datname=%s;")
            data = (database,)
            cur.execute(sql_str, data)
            return cur.fetchone()
    conn.close()

def db_create(params):
# Создание базы данных и 2-х таблиц client и telephon
    database = params['database']   
    if db_test(params) == None:
        conn = psycopg2.connect(database="postgres", user=params['user'], password=params['password'])
        conn.autocommit = True
        cur = conn.cursor()
        sql_str = sql.SQL("CREATE database {database};").format(database = sql.Identifier(database),)
        try:
            cur.execute(sql_str)
            result = f'База данных {database} создана на сервере.'
        except:
            raise Exception(f'База данных {database} не создана на сервере.')
        finally:
            conn.close()
    else:
        result = f'База данных {database} уже есть на сервере.'
        
    sql_str = "CREATE TABLE IF NOT EXISTS client"
    sql_str +="(id SERIAL PRIMARY KEY, "
    sql_str +="name VARCHAR(50) NOT NULL, "
    sql_str +="surname VARCHAR(50) NOT NULL, "
    sql_str +="email VARCHAR(50) UNIQUE NOT NULL);"
    db_execute(sql_str, '', params)

    sql_str = "CREATE TABLE IF NOT EXISTS telephon"
    sql_str +="(id SERIAL PRIMARY KEY, "
    sql_str +="number VARCHAR(50), "
    sql_str +="client_id integer NOT NULL REFERENCES client(id) ON DELETE CASCADE);"
    db_execute(sql_str, '', params)
    return result

def db_delete(params):
# Удаление базы данных
        conn = psycopg2.connect(database="postgres", user=params['user'], password=params['password'])
        conn.autocommit = True
        cur = conn.cursor()
        database = params['database']
        cur.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s;", (database,))
        sql_str = sql.SQL("DROP database IF EXISTS {database};").format(database = sql.Identifier(database),)
        try:
            cur.execute(sql_str)
            return f'База данных {database} удалена на сервере.'
        except:
            raise Exception(f'База данных {database} не удалена на сервере.')
        finally:
            conn.close()

# Функция соединения с базой данных.
def db_execute(sql_str, data, params, type=''):
    with psycopg2.connect(**params) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_str, data)
            if type == 'insert':
                result = cur.fetchone()
            elif type == 'select':
                result = cur.fetchall()
            else:
                result = ''
        return result
    conn.close()

# Функции управления базой данных клиентов.
def client_add(params, name, surname, email, number=None):
    sql_str = "INSERT INTO client(name, surname, email) VALUES(%s, %s, %s) RETURNING id;"
    data = (name, surname, email)
    client_id = db_execute(sql_str, data, params, 'insert')
    telephon_add(params, client_id, number)
    return client_id

def telephon_add(params, client_id, number):
    sql_str = "INSERT INTO telephon(client_id, number) VALUES(%s, %s) RETURNING id;"
    data = (client_id, number)
    telephon_id = db_execute(sql_str, data, params, 'insert')
    return telephon_id

def client_update(params, client_id, name, surname, email):
    sql_str = "SELECT name, surname, email FROM client WHERE id=%s;"
    data = (client_id,)
    clients = db_execute(sql_str, data, params, 'select')
    data_list = list((name, surname, email))
    for index, data in enumerate(data_list):
        if data is None:
            data_list[index] = clients[0][index]
    sql_str = "UPDATE client SET name=%s, surname=%s, email=%s WHERE id=%s;"
    data_list.append(client_id)
    db_execute(sql_str, data_list, params)
   
def telephon_update(params, client_id, number, number_new):
    sql_str = "UPDATE telephon SET number=%s WHERE number=%s AND client_id=%s;"
    data = (number_new, number, client_id)
    db_execute(sql_str, data, params)

def telephon_delete(params, client_id, number):
    sql_str = "DELETE FROM telephon WHERE number=%s AND client_id=%s;"
    data = (number, client_id)
    db_execute(sql_str, data, params)

def client_delete(params, client_id):
    sql_str = "DELETE FROM client WHERE id=%s;"
    data = (client_id,)
    db_execute(sql_str, data, params)

def client_find(params, name=None, surname=None, email=None, number=None):
    sql_str = "SELECT client.id, name, surname, email, number FROM client LEFT JOIN telephon ON telephon.client_id=client.id "
    sql_str += "WHERE (name = %(name)s OR %(name)s IS NULL) "
    sql_str += "AND (surname = %(surname)s OR %(surname)s IS NULL) "
    sql_str += "AND (email = %(email)s OR %(email)s IS NULL) "
    sql_str += "AND (number = %(number)s OR %(number)s IS NULL);"
    data = ({"name": name, "surname": surname, "email": email, "number": number})
    return db_execute(sql_str, data, params, 'select')

def client_all(params):
    sql_str = "SELECT client.id, name, surname, email, number FROM client LEFT JOIN telephon ON telephon.client_id=client.id;"
    data = ''
    return db_execute(sql_str, data, params, 'select')

def table_print(clients_list):
    my_table = PrettyTable()
    my_table.field_names = ["id", "Имя", "Фамилия", "Почта", "Телефон"]
    for client in clients_list:
        my_table.add_row(client)
    print(my_table)

if __name__ == '__main__':
# Программа для демонстрации функций управления базой данных в виде сценария.
    params = config('database.ini', 'postgresql')
    server_test(params)
    db_delete(params)
    print('Создание базы данных')
    print(db_create(params))

    name = 'Name_1'
    surname = 'Surname_1'
    email = 'Name_1@mail.ru'
    number = ''
    client_id = ''

    client_id = client_add(params, name, surname, email, number)
    print(f'Клиент {name} {surname} добавлен')

    name = 'Name_2'
    surname = 'Surname_2'
    email = 'Name_2@mail.ru'
    number = '1112'

    client_id = client_add(params, name, surname, email, number)
    print(f'Клиент {name} {surname} добавлен')

    name = 'Name_3'
    surname = 'Surname_3'
    email = 'Name_3@mail.ru'
    number = '1113'

    client_id = client_add(params, name, surname, email, number)
    print(f'Клиент {name} {surname} добавлен')

    client_id = '3'
    number = '1114'
    telephon_id = telephon_add(params, client_id, number)
    print(f'Клиенту {client_id} добавлен телефон {number}')

    client_id = '3'
    name = None
    surname = 'Surname_4'
    email = None
    number = None

    client_update(params, client_id, name, surname, email)
    print(f'Клиенту {client_id} изменена фамилия на {surname}')

    client_id = '3'
    number = '1114'
    number_new = '1115'
    telephon_update(params, client_id, number, number_new)
    print(f'Клиенту {client_id} изменен телефон {number} на {number_new}')

    print('Список клиентов:')
    table_print(client_all(params))

    client_id = None
    surname = 'Surname_4'
    email = None
    number = None
    print(f'Поиск клиента по по его данным: {name} {surname} {email} {number}')
    table_print(client_find(params, name, surname, email, number))

    client_id = '3'
    number = '1115'
    telephon_delete(params, client_id, number)
    print(f'Клиенту {client_id} удален телефон {number}')

    client_id = '3'
    client_delete(params, client_id)
    print(f'Клиент {client_id} удален')

    print('Список клиентов:')
    table_print(client_all(params))

    print('Удаление базы данных:')
    print(db_delete(params))
   