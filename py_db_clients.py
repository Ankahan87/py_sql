def return_conn(db="", usr="", pw=""):
    import psycopg2
    try:
        conn = psycopg2.connect(database=db, user=usr, password=pw)
        return conn
    except:
        return None
def create_clients_tables():
    conn = return_conn("clients_test", "postgres", "postgres")
    if conn != None:
        with conn.cursor() as cu:
            cu.execute("""CREATE TABLE IF NOT EXISTS Client (
                       id SERIAL PRIMARY KEY, 
                       firstname varchar(40) not null, 
                       surname varchar(60) not null, 
                       email varchar(60) not null);""")
            cu.execute("""CREATE TABLE IF NOT EXISTS Clients_phones (
                       id SERIAL PRIMARY KEY, 
                       phone_numper varchar(40) not null, 
                       clients_id INTEGER NOT NULL REFERENCES Client(id)
                       );""")
            conn.commit()
        conn.close()

def insert_client_data(name="", surname="", email="", phones=()):
    conn = return_conn("clients_test", "postgres", "postgres")
    if conn != None:
        if name !="" and surname !="" and email !="":
            with conn.cursor() as cu:
                cu.execute("""
                           SELECT id FROM client WHERE email = %s;
                           """, (email,))
                cl_by_email = cu.fetchone()
                if cl_by_email == None:
                    cu.execute("""INSERT into client(firstname, surname, email) 
                            values 
                            (%s, %s, %s);""", (name, surname, email))
                else:
                    cu.execute("""Update client
                               SET firstname = %s
                               WHERE email = %s;""", (name, email))
                    cu.execute("""Update client
                               SET surname = %s
                               WHERE email = %s;""", (surname, email))
                conn.commit()
                cu.execute("""
                           SELECT id FROM client WHERE firstname=%s AND surname=%s AND email = %s;
                           """, (name, surname, email))
                cl_id = cu.fetchone()[0]
                for pn in phones:
                    cu.execute("""INSERT into clients_phones(phone_numper, clients_id) 
                                values 
                                (%s, %s);""", (pn, cl_id))
                conn.commit()
        else:
            print("Для добавления нового Клиента в базу необходимо в обязательном порядке передавать Имя, Фамилию и e-mail")
        conn.close()

def insert_client_phones(email="", phones=()):
    conn = return_conn("clients_test", "postgres", "postgres")
    if conn != None:
        if email !="":
            with conn.cursor() as cu:
                cu.execute("""
                           SELECT id FROM client WHERE email = %s;
                           """, (email,))
                cl_id = cu.fetchone()[0]
                for pn in phones:
                    cu.execute("""
                            SELECT clients_id FROM clients_phones 
                            WHERE phone_numper = %s;
                            """, (pn,))
                    cl_probe = cu.fetchone()[0]
                    if cl_probe == None:
                        cu.execute("""INSERT into clients_phones(phone_numper, clients_id) 
                                    values 
                                    (%s, %s);""", (pn, cl_id))
                conn.commit()
        else:
            print("Для внесения телефонов Клиента в базу необходимо в обязательном порядке передавать его e-mail")
        conn.close()

def update_client_data(name="", surname="", email=""):
    conn = return_conn("clients_test", "postgres", "postgres")
    if conn != None:
        if (name !="" or surname !="") and email !="":
            with conn.cursor() as cu:
                if name !="":
                    cu.execute("""Update client
                               SET firstname = %s
                               WHERE email = %s;""", (name, email))
                elif surname !="":
                    cu.execute("""Update client
                               SET surname = %s
                               WHERE email = %s;""", (surname, email))
                conn.commit()
        else:
            print("Для изменения пользователя в базе необходимо в обязательном порядке указывать e-mail и Имя или Фамилию (для изменения)")
        conn.close()

def delete_client_phones(email="", phones=()):
    conn = return_conn("clients_test", "postgres", "postgres")
    if conn != None:
        if email !="":
            with conn.cursor() as cu:
                cu.execute("""
                           SELECT id FROM client WHERE email = %s;
                           """, (email,))
                cl_id = cu.fetchone()[0]
                for pn in phones:
                    cu.execute("""DELETE FROM clients_phones
                               WHERE phone_numper = %s and clients_id = %s;""", (pn, cl_id))
                conn.commit()
        else:
            print("Для удаления телефонов Клиента из базы необходимо в обязательном порядке передавать его e-mail")
        conn.close()

def delete_client(email=""):
    conn = return_conn("clients_test", "postgres", "postgres")
    if conn != None:
        if email !="":
            with conn.cursor() as cu:
                cu.execute("""
                           SELECT id FROM client WHERE email = %s;
                           """, (email,))
                cl_id = cu.fetchone()[0]
                cu.execute("""DELETE FROM clients_phones
                               WHERE clients_id = %s;""", (cl_id,))
                conn.commit()
                cu.execute("""DELETE FROM client
                               WHERE id = %s;""", (cl_id,))
                conn.commit()
        else:
            print("Для удаления Клиента из базы необходимо в обязательном порядке передавать его e-mail")
        conn.close()

def find_client(name="", surname="", email="", phone=None):
    client = None
    conn = return_conn("clients_test", "postgres", "postgres")
    if conn != None:
        with conn.cursor() as cu:
            if email != "":
                cu.execute("""
                                SELECT id, firstname, surname, email FROM client WHERE email = %s;
                                """, (email,))
                client = cu.fetchone()
            elif name !="" and surname !="":
                cu.execute("""
                            SELECT id, firstname, surname, email FROM client 
                            WHERE firstname = %s and surname = %s;
                            """, (name, surname))
                client = cu.fetchone()
            elif phone != None:
                cu.execute("""
                            SELECT clients_id FROM clients_phones 
                            WHERE phone_numper = %s;
                            """, (phone,))
                cl_id = cu.fetchone()[0]
                cu.execute("""
                            SELECT id, firstname, surname, email FROM client 
                            WHERE id = %s;
                            """, (cl_id,))
                client = cu.fetchone()
    return client

create_clients_tables()
insert_client_data("Ivan", "Ivanov", "test@t.ru", ("+73323442452", "894757583"))
insert_client_data("Petr", "Ivanov", "test_1@t.ru", ("+7325345643643"))
insert_client_data("Ivan", "Petrov", "test_2@t.ru", ())
insert_client_data("Ivan", "Ivanovich", "test@t.ru", ("+73323442452", "894757583"))
insert_client_data("Petr", "Ivankov", "test_6@t.ru", ("+73444444444", "894757583", "856585388356"))
update_client_data("Ivan", "Ivanov", "test@t.ru")
insert_client_phones("test_2@t.ru", ("994737347347", "8557575739845"))
delete_client_phones("test_2@t.ru", ("994737347347", "8557575739845"))
delete_client("test_6@t.ru")
print(find_client("", "", "test@t.ru"))


