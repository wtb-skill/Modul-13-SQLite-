from library_db import DBManager
from faker import Faker
import random

fake = Faker()


class DBDataGenerator:
    def __init__(self, db_manager):
        """Fills the db with random books data"""
        self.db_manager = db_manager
        self.genre_list = ['action', 'fantasy', 'sci-fi', 'thriller', 'horror', 'comedy', 'adventure']

    def generate_random_data(self) -> None:
        self.db_manager.create_tables()

        # Generate a list of genres to the genre table
        for genre in self.genre_list:
            self.db_manager.add_genre(genre_name=genre)

        # Generate random author data and add to the authors table
        for _ in range(10):
            first_name = fake.first_name()
            last_name = fake.last_name()
            biography = fake.text()
            self.db_manager.add_author(first_name, last_name, biography)

        # Generate random book data and add to the books table
        for _ in range(20):
            title = fake.catch_phrase()
            author_id = random.randint(1, 10)  # Assuming you have 10 authors in the database
            publication_year = fake.year()
            self.db_manager.add_book(title, author_id, publication_year)

        # Assign random genres to books
        book_ids = self.db_manager.get_book_ids()
        genre_ids = self.db_manager.get_genre_ids()
        self.db_manager.link_books_to_genres(book_ids, genre_ids)



