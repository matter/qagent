"""Factor protocol package.

Provides FactorBase, the factor loader, and built-in templates.
"""

from backend.factors.base import FactorBase  # noqa: F401
from backend.factors.loader import load_factor_from_code  # noqa: F401

__all__ = ["FactorBase", "load_factor_from_code"]
