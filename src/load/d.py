from dataclasses import dataclass

@dataclass
class Foo:
    i: int


f = Foo("1")
print(f)
