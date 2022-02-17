import json
from collections import Counter
from itertools import combinations, chain, islice
from math import comb

from sqlalchemy import create_engine, Column, Integer, BigInteger, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert

from rich.progress import Progress, BarColumn, TimeRemainingColumn


Base = declarative_base()


class Edge(Base):
    __tablename__ = "edgelist"

    a = Column(BigInteger, primary_key=True)
    b = Column(BigInteger, primary_key=True)
    count = Column(Integer)

    def __repr__(self):
        return f"Edge {self.a} {self.b}"


def chunks(iterable, size):
    iterator = iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, size - 1))


def filter_by_occurence(list_of_lists, cutoff):
    occurences = Counter(chain.from_iterable(list_of_lists))
    return [[i for i in sl if occurences[i] > cutoff] for sl in list_of_lists]


engine = create_engine(
    "postgresql+psycopg2://postgres:chevron-discolor-moonlit@localhost/edgelist",
    pool_pre_ping=True,
)
Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.create_all(engine)

with open("data.json", "r") as f:
    list_of_lists = json.load(f)
list_of_lists = filter_by_occurence(list_of_lists, 5)

with Progress(
    "[progress.description]{task.description}",
    BarColumn(),
    "{task.completed} of {task.total}",
    TimeRemainingColumn(),
) as progress:

    task1 = progress.add_task("Main process...", total=len(list_of_lists))
    task2 = progress.add_task("Sub process...")

    while not progress.finished:
        for item_list in list_of_lists:
            progress.update(task2, completed=0, total=comb(len(item_list), 2))
            chunksize = 10000
            for chunk in chunks(combinations(item_list, 2), chunksize):
                stmt = insert(Edge).values(
                    [
                        {"a": a, "b": b, "count": 1}
                        for a, b in (sorted(pair) for pair in chunk)
                    ]
                )
                stmt = stmt.on_conflict_do_update(
                    constraint=Edge.__table__.primary_key,
                    set_=dict(count=Edge.count + 1),
                )
                session.execute(stmt)
                session.commit()
                progress.update(task2, advance=chunksize)
            progress.update(task1, advance=1)

