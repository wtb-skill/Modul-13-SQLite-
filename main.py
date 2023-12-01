from library_db import DBManager
from db_data_generator import DBDataGenerator

if __name__ == '__main__':
    db_manager = DBManager("book_library.db")
    db_manager.create_tables()

    # Check if the database is empty
    if db_manager.is_database_empty():
        print("Database is empty. Generating data...")
        data_generator = DBDataGenerator(db_manager)
        data_generator.generate_random_data()
    else:
        print("Database is not empty. Skipping data generation.")

    # test db_manager.delete()
    # db_manager.delete(table='books')
    # db_manager.delete(table='authors')
    # db_manager.delete(table='genres')
    # db_manager.delete(table='book_genres')

    # test db_manager.update()
    # db_manager.update(table="author", _id=1, biography="test")
    # db_manager.update(table="book", _id=2, author_id=6)

    # test db_manager.select_all()
    # print(db_manager.select_all(table="books"))

    # test db_manager.select_where()
    # result = db_manager.select_where("books", publication_year="2006")
    # for row in result:
    #     print(row)

    # print all database information
    # db_manager.print_full_info()
