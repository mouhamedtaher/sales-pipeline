import csv
import random
from datetime import datetime, timedelta

products = [f"P{str(i).zfill(3)}" for i in range(1, 21)]
customers = [f"C{str(i).zfill(3)}" for i in range(1, 51)]
start_date = datetime(2025, 1, 1)

with open('data/sales_data.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['order_id', 'customer_id', 'product_id', 'quantity', 'price', 'order_date'])
    for i in range(1, 1001):
        order_id = f"O{str(i).zfill(4)}"
        customer_id = random.choice(customers)
        product_id = random.choice(products)
        quantity = random.randint(1, 5)
        price = round(random.uniform(10.0, 100.0), 2)
        order_date = (start_date + timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d')
        writer.writerow([order_id, customer_id, product_id, quantity, price, order_date])

print("Fichier data/sales_data.csv généré avec 1000 lignes.")
