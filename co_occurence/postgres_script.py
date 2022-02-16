from collections import defaultdict
from itertools import combinations
from math import comb

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert

from rich.progress import Progress


Base = declarative_base()


class Edge(Base):
    __tablename__ = "edgelist"

    a = Column(Integer, primary_key=True)
    b = Column(Integer, primary_key=True)
    count = Column(Integer)

    def __repr__(self):
        return f"Edge {self.a} {self.b}"


engine = create_engine(
    "postgresql+psycopg2://postgres:chevron-discolor-moonlit@localhost/edgelist"
)
Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.create_all(engine)

with Progress() as progress:

    task1 = progress.add_task("Main process...", total=len(list_of_lists))
    task2 = progress.add_task("Sub process...")

    while not progress.finished:
        for item_list in list_of_lists:

            progress.update(task2, completed=0, total=comb(len(item_list), 2))

            for a, b in combinations(item_list, 2):
                stmt = insert(Edge).values({"a": a, "b": b, "count": 1})
                stmt = stmt.on_conflict_do_update(
                    constraint=Edge.__table__.primary_key,
                    set_=dict(count=Edge.count + 1),
                )
                session.execute(stmt)
                progress.update(task2, advance=1)

            session.commit()
            progress.update(task1, advance=1)
