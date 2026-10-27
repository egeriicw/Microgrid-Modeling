"""Distributed energy resource models (solar PV, battery storage, EV charging).

Interfaces only. Each model maps (neighborhood, load) -> a net-load adjustment;
implementations land per docs/prd-suite-4d.md (Develop section). Keep every model deterministic
given its inputs and an explicit seed.
"""
from microgrid.der.base import DERModel, DERResult

__all__ = ["DERModel", "DERResult"]
