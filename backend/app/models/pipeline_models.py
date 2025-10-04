from pydantic import BaseModel, Field
from typing import List, Optional

class Source(BaseModel):
    file_id: str  # "sha256:<hex>"

class Neo4jCfg(BaseModel):
    uri: str
    user: str
    password: str

class RunRequest(BaseModel):
    mode: str = Field("auto", description="auto oder hybrid")
    neo4j: Optional[Neo4jCfg] = None
    sources: List[Source]
    prune: bool = True
    entity_resolution: bool = False

class RunResponse(BaseModel):
    job_id: str
    state: str

class StatusResponse(BaseModel):
    job_id: str
    state: str
    details: dict | None = None
