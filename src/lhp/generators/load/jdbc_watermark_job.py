"""Generator for JDBC watermark v2 extraction Job notebook.

Produces two artifacts from a single jdbc_watermark_v2 action:
1. An extraction notebook that uses WatermarkManager for HWM tracking
   and writes Parquet to a landing path (stored as auxiliary file)
2. A CloudFiles DLT stub that reads from the landing path (returned as
   the primary output, delegated to CloudFilesLoadGenerator)
"""

import logging
from typing import Any, Dict

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action

logger = logging.getLogger(__name__)


class JDBCWatermarkJobGenerator(BaseActionGenerator):
    """Generator for jdbc_watermark_v2 source type.

    Generates an extraction Job notebook (auxiliary file) and delegates
    the DLT pipeline file to CloudFilesLoadGenerator.
    """

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate extraction notebook and CloudFiles stub.

        Args:
            action: The jdbc_watermark_v2 load action.
            context: Generation context dict.

        Returns:
            CloudFiles DLT stub code (primary output).
        """
        raise NotImplementedError(
            "JDBCWatermarkJobGenerator.generate() not yet implemented"
        )
