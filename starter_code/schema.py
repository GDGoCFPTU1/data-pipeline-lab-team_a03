from pydantic import BaseModel, Field

# ==========================================
# ROLE 1: LEAD DATA ARCHITECT
# ==========================================

class UnifiedDocument(BaseModel):
    """
    Hệ thống cần 6 trường thông tin chuẩn (document_id, source_type, author, category, content, timestamp).

    Field choices:
    - Required: document_id, source_type, content (core identification)
    - Optional: author, category, timestamp (may be missing in raw data like doc2_corrupt.json)

    Note: This schema alone won't catch corrupt data (e.g., doc2_corrupt.json with "Null pointer exception").
    Quality gates in quality_check.py handle content validation.
    """
    document_id: str = Field(..., description="Unique document identifier")
    source_type: str = Field(..., description="Source type: PDF or Video")
    author: str = Field(default="", description="Author of the document")
    category: str = Field(default="", description="Document category")
    content: str = Field(..., description="Extracted content or transcript")
    timestamp: str = Field(default="", description="Creation timestamp")
