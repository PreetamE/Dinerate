# 🍽️ DineRate – Mini Data Engineering Project

DineRate is a mini data engineering project built using **PySpark** and **PostgreSQL**. It simulates a real-world restaurant review pipeline, demonstrating end-to-end data extraction, transformation, loading (ETL), and storage in a relational database.

---

## 📂 Project Structure

```
dinerate/
├── config.py
├── main.py
├── readers/
│   └── read_data.py
├── transformers/
│   ├── clean_users.py
│   ├── clean_reviews.py
│   └── clean_restaurants.py
├── joiners/
│   └── join_all.py
└── writers/
    └── write_output.py
```

---

## 🧪 Data Sources

- `users.csv` – 1000 users
- `restaurants.csv` – 100 restaurants
- `reviews.csv` – 5000 reviews

---

## ⚙️ Technologies Used

- Python 3.11
- PySpark
- Railway.app
- PostgreSQL
- JDBC Driver


---

## 🛠️ Features

- Null and missing value handling
- Custom rating mapping (e.g., "bad" → 1)
- Slug generation from restaurant names
- Address parsing into `street`, `city`, and `state`
- Category tagging based on rating (Best, Good, Not Recommended)
- Full joins and write to PostgreSQL

---

## 🚀 How to Run

1. Clone the repo:
   ```bash
   git clone https://github.com/PreetamE/dinerate.git
   cd dinerate
   ```

2. Create PostgreSQL database:
   ```sql
   CREATE DATABASE dinerate;
   ```

3. Run the pipeline:
   ```bash
   python main.py
   ```
4. Cloud Database Access (Railway)

The final joined data is loaded into a PostgreSQL database hosted on Railway.
Table Name: dinerate_full_data

🔗 Connection Parameters (to be shared securely):
1) Host
2) Port
3) Database
4) User
5) Password
6) JDBC URL

Please reach out to me for actual credentials.
---

## 💡 Author

**Preetam Epari**   
GitHub: https://github.com/PreetamE
---

