"""Pipeline configuration models for incremental load features."""

from enum import Enum

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
