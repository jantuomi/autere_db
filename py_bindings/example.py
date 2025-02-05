from pprint import pformat, pprint
import log_db
from log_db import Value, Type, Bound

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

# Create a new database
db = log_db.DB \
    .configure() \
    .data_dir("db") \
    .schema([
        ("id", Type.int()),
        ("name", Type.string()),
    ]) \
    .primary_key("id") \
    .secondary_keys(["name"]) \
    .initialize()

#db.upsert(Inst(1, "foo").into_record())

#res = db.find_by("name", log_db.Value.string("foo"))
#insts = [Inst.from_record(r) for r in res]
res = db.range_by("name",
    Bound.unbounded(),
    Bound.unbounded(),
)
# insts = [Inst.from_record(r) for r in res]

print("before delete:")
res = db.find_by("name", Value.string("foo"))
print(len(res))

res = db.delete_by("name", Value.string("foo"))
#res = db.delete_by("id", Value.int(1))

print("deleted:")
for r in res:
    pprint(Inst.from_record(r))

print("find name after delete:")
res = db.find_by("name", Value.string("foo"))
print(len(res))

print("find id after delete:")
res = db.find_by("id", Value.int(1))
print(len(res))
