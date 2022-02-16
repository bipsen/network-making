from pathlib import Path
from collections import defaultdict
from itertools import combinations
from math import comb

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from rich.progress import Progress


Base = declarative_base()


class Edge(Base):
    __tablename__ = "edgelist"

    a = Column(String, primary_key=True)
    b = Column(String, primary_key=True)
    count = Column(Integer)

    def __repr__(self):
        return f"Edge {self.a} {self.b}"


engine = create_engine("sqlite:///edgelist.db")

Session = sessionmaker(bind=engine)
session = Session()


Base.metadata.create_all(engine)

d = defaultdict(int)

list_of_lists = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]

with Progress() as progress:

    task1 = progress.add_task("[red]Main process...", total=len(list_of_lists))
    task2 = progress.add_task("[green]Sub process...")

    while not progress.finished:
        for item_list in list_of_lists:

            progress.update(task2, total=comb(len(item_list), 2))

            for pair in combinations(item_list, 2):
                a, b = sorted(pair)
                q = session.query(Edge).filter_by(a=a, b=b)
                if q.first():
                    q.update({Edge.count: Edge.count + 1})
                else:
                    edge = Edge(a=a, b=b, count=1)
                    session.add(edge)

                progress.update(task2, advance=1)

            session.commit()

            progress.update(task1, advance=1)
            progress.update(task2, completed=0)
