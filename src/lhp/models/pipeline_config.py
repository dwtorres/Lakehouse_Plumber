"""Pipeline configuration models for incremental load features."""

import warnings
from enum import Enum
from typing import Optional

# Suppress Pydantic warning about 'schema' field shadowing BaseModel.schema() class method.
# This is deliberate: 'schema' is a UC namespace field, not related to Pydantic's schema().
warnings.filterwarnings(
    "ignore", message=r".*Field name \"schema\".*shadows an attribute.*"
)

from pydantic import BaseModel, Field, field_validator


class WatermarkType(str, Enum):
    """Supported watermark column types for incremental extraction."""

    TIMESTAMP = "timestamp"
    NUMERIC = "numeric"


class WatermarkConfig(BaseModel):
    """Watermark configuration for incremental loads.

    Defines the column and comparison strategy for high-water mark tracking.
    The load generator reads MAX(column) from the Bronze target table and
    filters the JDBC source query using the specified operator.
    """

    column: str = Field(..., description="Column name for high-water mark tracking")
    type: WatermarkType = Field(..., description="Watermark column data type")
    operator: str = Field(
        ">=",
        description="Comparison operator for watermark filter",
    )
    # v2 fields for WatermarkManager-based tracking
    source_system_id: Optional[str] = Field(
        None, description="Source system identifier for WatermarkManager"
    )
    catalog: Optional[str] = Field(
        None, description="Unity Catalog name for watermark tracking table"
    )
    schema: Optional[str] = Field(
        None, description="Schema name for watermark tracking table"
    )

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: str) -> str:
        """Validate operator is >= or >.

        Args:
            v: The operator value to validate.

        Returns:
            The validated operator string.

        Raises:
            ValueError: If operator is not '>=' or '>'.
        """
        if v not in (">=", ">"):
            raise ValueError(f"operator must be '>=' or '>', got '{v}'")
        return v
