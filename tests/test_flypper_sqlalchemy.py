from pytest import fixture

from sqlalchemy.orm.session import sessionmaker
from flypper_sqlalchemy.storage.sqla import SqlAlchemyStorage

from flypper.entities.flag import UnversionedFlagData
from sqlalchemy import (
    create_engine,
    MetaData,
)

# Setup a test database, in memory
engine = create_engine('sqlite://')
metadata = MetaData()

# Add flypper-sqlalchemy tables to the database
flypper_flags = SqlAlchemyStorage.build_metadata_table(
    sqla_metadata=metadata
)
flypper_metadata = SqlAlchemyStorage.build_flags_table(
    sqla_metadata=metadata
)
metadata.create_all(engine)

Session = sessionmaker(bind=engine)

def test_empty_storage(storage):
    assert storage.list() == []

def test_upsert(storage):
    storage.upsert(flag_data("a"))
    flags = storage.list()
    assert len(flags) == 1
    assert flags[0].name == "a"

def test_upsert_with_increasing_versions(storage):
    storage.upsert(flag_data("a"))
    storage.upsert(flag_data("b"))
    flags = storage.list()
    assert flags[0].version == 1
    assert flags[0].name == "a"
    assert flags[1].version == 2
    assert flags[1].name == "b"

def test_list_with_version(storage):
    storage.upsert(flag_data("a"))
    assert len(storage.list(version__gt=0)) == 1
    assert len(storage.list(version__gt=1)) == 0

def test_delete(storage):
    storage.upsert(flag_data("a"))
    storage.delete(flag_name="a")
    assert len(storage.list()) == 0


@fixture(params=["session", "engine"])
def storage(request):
    # Cleaning up the database
    connection = engine.connect()
    with connection.begin():
        connection.execute(flypper_flags.delete())
        connection.execute(flypper_metadata.delete())

    if request.param == "session":
        return SqlAlchemyStorage(session=Session())
    elif request.param == "engine":
        return SqlAlchemyStorage(engine=engine)
    else:
        raise ValueError(f"Unsupported param: '{request.param}'")


def flag_data(name: str) -> UnversionedFlagData:
    return {
        "name": name,
        "enabled": True,
        "enabled_for_actors": {
            "actor_key": "user_id",
            "actor_ids": ["8", "42", "200000"],
        },
        "enabled_for_percentage_of_actors": None,
        "deleted": False,
    }
