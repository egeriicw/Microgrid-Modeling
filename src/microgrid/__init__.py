"""Microgrid Planner.

    microgrid
    |-- core            shared contracts (Structure, Neighborhood, LoadResult)
    |-- community_load  the Community Load Profiler: what load does this community have?
    |   |-- geo         GIS footprints -> classified Neighborhood
    |   |-- synthetic   SNLG: Monte-Carlo ResStock/ComStock load ensembles
    |   `-- sources     LoadSource adapters (synthetic, measured)
    |-- der             solar / storage / EV models (interfaces)
    |-- dispatch        operating strategies (interfaces)
    |-- economics       financial models (interfaces)
    `-- study           pipeline chaining the above

Dependencies point downward only; enforced by tests/test_architecture.py.
"""
__version__ = "0.1.0"
