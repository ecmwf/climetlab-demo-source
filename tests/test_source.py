import os
import sqlite3

from climetlab_demo_source import source

DATA = [
    (50, 3.3, "2001-01-01 00:00:00", 4.9),
    (51, -3, "2001-01-02 00:00:00", 7.3),
    (50.5, -1.8, "2001-01-03 00:00:00", 5.5),
]


def make_db():
    if os.path.exists("test.db"):
        os.unlink("test.db")

    conn = sqlite3.connect("test.db")
    c = conn.cursor()
    c.execute(
        """CREATE TABLE data(
                    lat NUMBER,
                    lon NUMBER,
                    time TEXT,
                    value NUMBER)"""
    )
    c.executemany("INSERT INTO data VALUES(?,?,?,?);", DATA)
    conn.commit()


def test_source():
    make_db()

    s = source("sqlite:///test.db", "select * from data;", parse_dates=["time"])
    df = s.to_pandas()
    print(df)


if __name__ == "__main__":
    test_source()
