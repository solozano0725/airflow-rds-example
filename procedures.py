import utils.Connection as conn

def createTable():
    q = """DROP TABLE IF EXISTS monitoringtable.info;
    CREATE TABLE monitoringtable.info (id INT,h INT,m INT,a INT,d DATE);"""
    self.connection = conn(db_name, db_user, db_password, db_host, db_port)
    self.connection.execute_query(query)

def insertData():    
    now = datetime.now()
    print("now =", now)

    id = int(randrange(10))
    d = now.strftime("%Y-%m-%d")
    h = int(now.strftime("%H"))
    m = int(now.strftime("%M"))
    s = int(now.strftime("%S"))

    q= """
    INSERT INTO monitoringtable.info (id INT,h INT,m INT,a INT,d DATE)
    VALUES ({id},{h},{m},{s},{d});
    """
    query = query.format( 
        id = id,
        h = h,
        m = m,
        s = s,
        d = d) 
    self.connection.execute_query(query)