import pandas as pd
from sqlalchemy import create_engine, ForeignKey, UniqueConstraint, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

Base = declarative_base()


class Molecule(Base):
    __tablename__ = "molecules"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    max_phase = Column(String(255))
    structure = Column(String(255))
    inchi_key = Column(String(255))
    __table_args__ = (UniqueConstraint('inchi_key'),)


class Target(Base):
    __tablename__ = "targets"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    organism = Column(String(255))


class Activity(Base):
    __tablename__ = "activities"
    id = Column(Integer, primary_key=True)
    type = Column(String(255))
    units = Column(String(255))
    value = Column(String(255))
    relation = Column(String(255))
    molecule_id = Column(Integer, ForeignKey(Molecule.id), primary_key=True)
    target_id = Column(Integer, ForeignKey(Target.id), primary_key=True)


class Connection:
    engine = None

    def setup_database(self, dburl, echo):
        self.engine = create_engine(dburl, echo=echo)
        Base.metadata.create_all(self.engine)

    def test_bulk_insert_mappings(self, model, map_list):
        session = Session(bind=self.engine)
        if model == "molecule":
            id_and_inchi = session.query(Molecule.id, Molecule.inchi_key).all()
            existing_id = [row[0] for row in id_and_inchi]
            existing_inchi = [row[1] for row in id_and_inchi]
            filtered = [val for val in map_list if
                        val["inchi_key"] not in existing_inchi and val["id"] not in existing_id]
            session.bulk_insert_mappings(Molecule, filtered)

        if model == "target":
            existing_id = [row[0] for row in session.query(Target.id).all()]
            filtered = [val for val in map_list if val["id"] not in existing_id]
            session.bulk_insert_mappings(Target, filtered)

        if model == "activity":
            existing_id = [row[0] for row in session.query(Activity.id).all()]
            filtered = [val for val in map_list if val["id"] not in existing_id]
            session.bulk_insert_mappings(Activity, filtered)

        session.commit()

    def df_to_db(self, file_list):
        for val in file_list:
            df = pd.read_csv(f"data/{val}.csv")
            df.to_sql(val, self.engine, if_exists='append')

    def fetch_count(self):
        session = Session(bind=self.engine)
        count_report = {
            "target": session.query(Target).count(),
            "molecules": session.query(Molecule).count(),
            "activities": session.query(Activity).count()
        }
        return count_report


if __name__ == '__main__':
    conn = Connection()
    conn.setup_database("sqlite:///dags/ebi_processed.db", False)
