class PetDB:
    def __init__(self, file):
        self.conn = sqlite3.connect(file)
        self.cur = self.conn.cursor()
        
    def create_table(self):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS pets (
                name text,
                species text,
                color text,
                age int,
                human text
            )
        """

        self.cur.execute(create_table_query); 
        self.conn.commit()
        
    def drop_table(self):
        self.cur.execute("DROP TABLE IF EXISTS pets")
        self.conn.commit()

    def add_pet(self, name, species, color, age, human):
        query =  f"""
            INSERT INTO pets
            (name, species, color, age, human)
            VALUES ("{name}", "{species}", "{color}", {age}, "{human}")
        """
        
        self.cur.execute(query)
        self.conn.commit()
    
    def get_all(self):
        self.cur.execute("SELECT * FROM pets")
        return pd.DataFrame(
            self.cur.fetchall(),
            columns=["name", "species", "color", "age", "human"]
        )
        
    def get_by_name(self, name):
        self.cur.execute("SELECT * FROM pets WHERE name = ?", (name,))
        return pd.DataFrame(
            self.cur.fetchall(),
            columns=["name", "species", "color", "age", "human"]
        )
    
    def had_birthday(self, name):
        old_age = self.get_by_name(name).age[0]
        
        update_query = f"""
        UPDATE pets
        SET age = {old_age + 1}
        WHERE name = "{name}"
        """
        
        self.cur.execute(update_query)
        self.conn.commit()
        
    def delete_by_name(self, name):
        self.cur.execute("DELETE FROM pets WHERE name = ?", (name,))
        self.conn.commit()