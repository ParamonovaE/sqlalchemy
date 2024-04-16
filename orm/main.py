import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker
import json
from model import create_tables, Publisher, Shop, Book, Stock, Sale

DSN = "postgresql://postgres:postgres@localhost:5432/orm"
engine = sq.create_engine(DSN)
create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

json_file_path = 'fixtures/tests_data.json'

with open(json_file_path, 'r', encoding='utf-8') as file:
    data = json.load(file)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()

publisher_input = input("Введите имя или ID издателя: ")
try:
    publisher_id = int(publisher_input)
    publisher = session.query(Publisher).filter(Publisher.id == publisher_id).all()
except ValueError:
    publisher = session.query(Publisher).filter(Publisher.name == publisher_input).all()

if publisher:
    publisher = publisher[0]
    sales_info = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale) \
                        .join(Book.stocks) \
                        .join(Stock.sales) \
                        .join(Stock.shop) \
                        .filter(Book.id_publisher == publisher.id).all()
    for sale in sales_info:
        print(f"{sale[0]} | {sale[1]} | {sale[2]} | {sale[3]}")
else:
    print("Издатель не найден.")

session.close()


