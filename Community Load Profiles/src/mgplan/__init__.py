"""Community microgrid planning suite (mgplan).

Layers, bottom to top:
  core      shared data contracts (Structure, Neighborhood, LoadResult)
  geo       GIS ingestion: structure footprints -> classified Neighborhood
  load      load sources (synthetic via snlg, or measured) for a Neighborhood
  der       distributed energy resource models (solar, storage, EV): interfaces
  dispatch  operating-strategy models: interfaces
  economics financial models: interfaces
  study     pipeline that chains the above into one reproducible Study

`snlg` remains the synthetic load engine; mgplan depends on it, never the reverse.
"""
__version__ = "0.1.0"
