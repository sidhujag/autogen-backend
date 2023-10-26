# autogen agent/function backend
Minimalist fastAPI backend using qdrant/cohere/mongodb for semantic agent/functions lookup with db read/write of agent/functions metadata. Rename .env.example to .env and set the following variables:

MONGODB_PW: Database for reading/writing functions and agents metadata
QDRANT_API_KEY: Semantic lookup of agents/functions (always lookup 10)
QDRANT_URL: Your QDrant cloud URL
COHERE_API_KEY: Rerank semantic lookups to output of 3
