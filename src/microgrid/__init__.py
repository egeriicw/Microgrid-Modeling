"""Microgrid Planner.

    microgrid
    |-- core            cross-cutting contract (LoadResult)
    |-- geo             geography: structures, locations, neighborhoods (the reference layer)
    |-- community_load  Community Load Profiler: what load does this geography have?
    |   |-- synthetic   SNLG: Monte-Carlo ResStock/ComStock load ensembles
    |   |-- categorize  geography -> SNLG archetype categories
    |   `-- sources     LoadSource adapters (synthetic, measured)
    |-- der             solar / storage / EV models (interfaces)
    |-- dispatch        operating strategies (interfaces)
    |-- economics       financial models (interfaces)
    `-- study           pipeline chaining the above

geo and community_load are parallel siblings; everything else reads geo.
Dependencies point downward only; enforced by tests/test_architecture.py.
"""
__version__ = "0.1.0"
