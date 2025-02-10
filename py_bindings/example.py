import tempfile
from pprint import pformat, pprint
from log_db import Value, Bound, DB

# TODO: This should be imported from the log_db module
# but exporting type aliases does not work automatically
Record = list[Value]

class Inst:
    def __init__(self, id: int, name: str):
        self.id = id
        self.name = name

    def into_record(self) -> Record:
        return [
            Value.int(self.id),
            Value.string(self.name),
        ]

    @staticmethod
    def from_record(rec: Record):
        return Inst(
            rec[0].as_int(),
            rec[1].as_string(),
        )

    def __repr__(self) -> str:
        return pformat(self.__dict__)

temp_dir = tempfile.TemporaryDirectory()

# Create a new database
db = DB \
    .configure() \
    .data_dir(temp_dir.name) \
    .fields(["id", "name"]) \
    .primary_key("id") \
    .secondary_keys(["name"]) \
    .initialize()

db.upsert(Inst(1, "foo").into_record())

#res = db.find_by("name", log_db.Value.string("foo"))
#insts = [Inst.from_record(r) for r in res]
res = db.range_by("name",
    Bound.unbounded(),
    Bound.unbounded(),
)
# insts = [Inst.from_record(r) for r in res]

res = db.find_by("name", Value.string("foo"))
print("before delete count:", len(res))

res = db.delete_by("name", Value.string("foo"))
#res = db.delete_by("id", Value.int(1))

print("deleted instances:")
for r in res:
    pprint(Inst.from_record(r))

res = db.find_by("name", Value.string("foo"))
print("find name after delete count:", len(res))

res = db.find_by("id", Value.int(1))
print(f"find id after delete count: {len(res)}")

res = db.delete_by("name", Value.string("bar"))

db.tx_begin()
db.upsert(Inst(2, "bar").into_record())
db.tx_rollback()

res = db.find_by("name", Value.string("bar"))
print("find name after rollback count:", len(res))

db.tx_begin()
db.upsert(Inst(2, "bar").into_record())
db.tx_commit()

res = db.find_by("name", Value.string("bar"))
print("find name after commit count:", len(res))
