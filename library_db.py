import sqlite3
from sqlite3 import Error
import random
from typing import List, Tuple, Optional, Any


class DBManager:
    """Class managing the SQLite database operations for books, authors, genres, and their associations."""
    def __init__(self, db_file):
        """Initializes the DBManager object with the specified database file."""
        self.db_file = db_file
        self.conn = self.create_connection()

    def create_connection(self) -> Optional[sqlite3.Connection]:
        """Creates a database connection."""
        try:
            conn = sqlite3.connect(self.db_file)
            return conn
        except Error as e:
            print(e)
            return None

    def execute_sql(self, sql: str, values: Optional[Tuple[Any, ...]] = None) -> None:
        """Executes SQL queries on the database."""
        try:
            c = self.conn.cursor()
            if values:
                c.execute(sql, values)
            else:
                c.execute(sql)
            self.conn.commit()
        except Error as e:
            print(e)

    def create_tables(self) -> None:
        """Creates necessary tables if they don't exist in the database."""
        create_books_sql = """
        -- books table
        CREATE TABLE IF NOT EXISTS books (
          book_id INTEGER PRIMARY KEY,
          title TEXT NOT NULL,
          author_id INTEGER NOT NULL,
          publication_year TEXT,
          FOREIGN KEY (author_id) REFERENCES authors (author_id)
        );
        """

        create_authors_sql = """
        -- authors table
        CREATE TABLE IF NOT EXISTS authors (
          author_id INTEGER PRIMARY KEY,
          first_name TEXT NOT NULL,
          last_name TEXT NOT NULL,
          biography TEXT
        );
        """

        create_genres_sql = """
        -- genres table
        CREATE TABLE IF NOT EXISTS genres (
          genre_id INTEGER PRIMARY KEY,
          genre_name TEXT NOT NULL
        );
        """

        create_book_genres_sql = """
        -- junction table for books and genres (many-to-many)
        CREATE TABLE IF NOT EXISTS book_genres (
          book_id INTEGER,
          genre_id INTEGER,
          FOREIGN KEY (book_id) REFERENCES books (book_id),
          FOREIGN KEY (genre_id) REFERENCES genres (genre_id),
          PRIMARY KEY (book_id, genre_id)
        );
        """

        self.execute_sql(create_books_sql)
        self.execute_sql(create_authors_sql)
        self.execute_sql(create_genres_sql)
        self.execute_sql(create_book_genres_sql)

    def add_book(self, title: str, author_id: int, publication_year: str) -> None:
        """Adds a book entry to the database."""
        sql = """
        INSERT INTO books(title, author_id, publication_year)
        VALUES(?, ?, ?)
        """
        books_data = (title, author_id, publication_year)
        self.execute_sql(sql, books_data)

    def add_author(self, first_name: str, last_name: str, biography: str) -> None:
        """Adds an author entry to the database."""
        sql = """
        INSERT INTO authors(first_name, last_name, biography)
        VALUES(?, ?, ?)
        """
        author_data = (first_name, last_name, biography)
        self.execute_sql(sql, author_data)

    def genre_exists(self, genre_name: str) -> bool:
        """Checks if a genre exists in the database."""
        try:
            c = self.conn.cursor()
            c.execute("SELECT genre_id FROM genres WHERE genre_name = ?", (genre_name,))
            genre_id = c.fetchone()
            return genre_id is not None
        except Error as e:
            print(e)
            return False

    def add_genre(self, genre_name: str) -> None:
        """Adds a genre entry to the database if it doesn't already exist."""
        if not self.genre_exists(genre_name):
            sql = """
            INSERT INTO genres(genre_name)
            VALUES(?)
            """
            genre_data = (genre_name,)
            self.execute_sql(sql, genre_data)
        else:
            print(f"Genre '{genre_name}' already exists in the database.")

    def link_books_to_genres(self, book_ids: List[int], genre_ids: List[int]) -> None:
        """Links books with random genres in the database."""
        for book_id in book_ids:
            random_genres = random.sample(genre_ids, k=random.randint(1, min(len(genre_ids), 3)))
            for genre_id in random_genres:
                self.execute_sql("INSERT INTO book_genres (book_id, genre_id) VALUES (?, ?)", (book_id, genre_id))

    def get_book_ids(self) -> List[int]:
        """Retrieves IDs of all books from the database."""
        try:
            c = self.conn.cursor()
            c.execute("SELECT book_id FROM books")
            book_ids = [row[0] for row in c.fetchall()]
            return book_ids
        except Error as e:
            print(e)
            return []

    def get_genre_ids(self) -> List[int]:
        """Retrieves IDs of all genres from the database."""
        try:
            c = self.conn.cursor()
            c.execute("SELECT genre_id FROM genres")
            genre_ids = [row[0] for row in c.fetchall()]
            return genre_ids
        except Error as e:
            print(e)
            return []

    def is_database_empty(self) -> bool:
        """Checks if the database tables are empty."""
        try:
            c = self.conn.cursor()

            # Check if the books table is empty
            c.execute("SELECT COUNT(*) FROM books")
            book_count = c.fetchone()[0]

            # Check if the authors table is empty
            c.execute("SELECT COUNT(*) FROM authors")
            author_count = c.fetchone()[0]

            # Check if the genres table is empty
            c.execute("SELECT COUNT(*) FROM genres")
            genre_count = c.fetchone()[0]

            # Check if the book_genres junction table is empty
            c.execute("SELECT COUNT(*) FROM book_genres")
            book_genre_count = c.fetchone()[0]

            # If all tables are empty, return True
            return all(count == 0 for count in [book_count, author_count, genre_count, book_genre_count])

        except Error as e:
            print(e)
            return False

    def delete(self, table: str, id: Optional[int] = None) -> None:
        """Deletes records from specified tables in the database."""
        try:
            c = self.conn.cursor()

            delete_queries = {
                "books": ("DELETE FROM books WHERE book_id = ?", "DELETE FROM books"),
                "authors": ("DELETE FROM authors WHERE author_id = ?", "DELETE FROM authors"),
                "genres": ("DELETE FROM genres WHERE genre_id = ?", "DELETE FROM genres"),
                "book_genres": ("DELETE FROM book_genres WHERE book_id = ?", "DELETE FROM book_genres")
            }

            if table in delete_queries:
                query = delete_queries[table]
                if id:
                    c.execute(query[0], (id,))
                else:
                    c.execute(query[1])

            self.conn.commit()
        except Error as e:
            print(e)

    def update(self, table: str, _id: int, **kwargs: Any) -> None:
        """Updates records in the specified table."""
        parameters = [f"{k} = ?" for k in kwargs]
        parameters = ", ".join(parameters)
        values = tuple(v for v in kwargs.values())
        values += (_id,)

        sql = f'''UPDATE {table}s
                 SET {parameters}
                 WHERE {table}_id = ?'''

        try:
            cur = self.conn.cursor()
            cur.execute(sql, values)
            self.conn.commit()
            print("Update successful")
        except sqlite3.OperationalError as e:
            print(e)

    def select_all(self, table: str) -> List[Tuple[Any, ...]]:
        """
        Query all rows in the table
        :param table: Table name
        :return: All rows in the table
        """
        try:
            cur = self.conn.cursor()
            cur.execute(f"SELECT * FROM {table}")
            rows = cur.fetchall()
            return rows
        except Error as e:
            print(e)
            return []

    def select_where(self, table: str, **query: Any) -> List[Tuple[Any, ...]]:
        """
        Query tasks from table with data from **query dict
        :param table: table name
        :param query: dict of attributes and values
        :return: rows matching the query conditions
        """
        try:
            cur = self.conn.cursor()
            qs = []
            values = ()
            for k, v in query.items():
                qs.append(f"{k}=?")
                values += (v,)

            q = " AND ".join(qs)
            cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
            rows = cur.fetchall()
            return rows
        except Error as e:
            print(e)
            return []

    def print_full_info(self) -> None:
        """Prints comprehensive information about books, authors, and their associations."""
        try:
            cur = self.conn.cursor()

            # Joining books, authors, and book_genres tables to fetch comprehensive information
            cur.execute("""
                SELECT 
                    books.book_id, 
                    books.title, 
                    authors.first_name, 
                    authors.last_name, 
                    authors.biography, 
                    books.publication_year, 
                    GROUP_CONCAT(genres.genre_name, ', ') AS book_genres
                FROM 
                    books
                INNER JOIN authors ON books.author_id = authors.author_id
                LEFT JOIN book_genres ON books.book_id = book_genres.book_id
                LEFT JOIN genres ON book_genres.genre_id = genres.genre_id
                GROUP BY 
                    books.book_id
                ORDER BY 
                    books.book_id
            """)

            rows = cur.fetchall()

            # Printing the collected information
            for row in rows:
                print(f"Book ID: {row[0]}")
                print(f"Book Title: {row[1]}")
                print(f"Author Name: {row[2]} {row[3]}")
                print(f"Author Bio: {row[4]}")
                print(f"Published Date: {row[5]}")
                print(f"Genres: {row[6]}\n")

        except Error as e:
            print(e)
