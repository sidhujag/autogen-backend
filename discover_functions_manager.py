import time
import json
import os
import random
import logging
import traceback
import cachetools.func

from dotenv import load_dotenv
from qdrant_client import QdrantClient
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field
from qdrant_client.http import models as rest
from langchain.vectorstores import Qdrant
from langchain.embeddings import OpenAIEmbeddings
from qdrant_retriever import QDrantVectorStoreRetriever
from langchain.retrievers import ContextualCompressionRetriever
from cohere_rerank import CohereRerank
from langchain.schema import Document
from datetime import datetime, timedelta
from qdrant_client.http.models import PayloadSchemaType
from functions_and_agents_metadata import AuthAgent

class DiscoverFunctionsModel(BaseModel):
    query: Optional[str] = None
    category: str
    auth: AuthAgent
    def __str__(self):
        if self.query:
            return self.query + self.auth + self.category
        else:
            return self.auth + self.category

    def __eq__(self,other):
        if self.query:
            return self.query == other.query and self.auth == other.auth and self.category == other.category
        else:
            return self.auth == other.auth and self.category == other.category

    def __hash__(self):
        return hash(str(self))

class DiscoverFunctionsManager:

    def __init__(self, rate_limiter, rate_limiter_sync):
        load_dotenv()  # Load environment variables
        self.QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
        os.getenv("COHERE_API_KEY")
        self.QDRANT_URL = os.getenv("QDRANT_URL")
        self.index = None
        self.rate_limiter = rate_limiter
        self.rate_limiter_sync = rate_limiter_sync
        self.max_length_allowed = 512
        self.collection_name = "discover_functions"
        self.client = QdrantClient(url=self.QDRANT_URL, api_key=self.QDRANT_API_KEY)
        self.inited = False
        
        
    def create_new_functions_retriever(self, api_key: str):
        """Create a new vector store retriever unique to the agent."""
        # create collection if it doesn't exist (if it exists it will fall into finally)
        try:
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=rest.VectorParams(
                    size=1536,
                    distance=rest.Distance.COSINE,
                ),
            )
            self.client.create_payload_index(self.collection_name, "metadata.namespace_id", field_schema=PayloadSchemaType.KEYWORD)
        except:
            logging.info(f"DiscoverFunctionsManager: loaded from cloud...")
        finally:
            logging.info(
                f"DiscoverFunctionsManager: Creating memory store with collection {self.collection_name}")
            vectorstore = Qdrant(self.client, self.collection_name, OpenAIEmbeddings(openai_api_key=api_key))
            compressor = CohereRerank()
            compression_retriever = ContextualCompressionRetriever(
                base_compressor=compressor, base_retriever=QDrantVectorStoreRetriever(
                    rate_limiter=self.rate_limiter, rate_limiter_sync=self.rate_limiter_sync, collection_name=self.collection_name, client=self.client, vectorstore=vectorstore,
                )
            )
            return compression_retriever

    def transform(self, namespace_id, data, category):
        """Transforms function data for a specific category."""
        now = datetime.now().timestamp()
        result = []
        for item in data:
            page_content = {'name': item['name'], 'category': category, 'description': str(
                item['description'])}
            lenData = len(str(page_content))
            if lenData > self.max_length_allowed:
                logging.info(
                    f"DiscoverFunctionsManager: transform tried to create a function that surpasses the maximum length allowed max_length_allowed: {self.max_length_allowed} vs length of data: {lenData}")
                continue
            metadata = {
                "id":  random.randint(0, 2**32 - 1),
                "namespace_id": namespace_id,
                "extra_index": category,
                "last_accessed_at": now,
            }
            doc = Document(
                page_content=json.dumps(page_content),
                metadata=metadata
            )
            result.append(doc)
        return result

    def extract_name_and_category(self, documents):
        result = []
        seen = set()  # Track seen combinations of name and category
        for doc in documents:
            # Parse the page_content string into a Python dict
            text = json.loads(doc.page_content)
            name = text.get('name')
            category = text.get('category')

            # Check if this combination has been seen before
            if (name, category) not in seen:
                result.append({'name': name, 'category': category})
                seen.add((name, category))  # Mark this combination as seen

        return result
    
    async def pull_functions(self, function_input: DiscoverFunctionsModel):
        """Fetch functions based on a query."""
        start = time.time()
        if self.inited is False:
            try:
                self.client.get_collection(self.collection_name)
            except Exception as e:
                logging.warn(f"DiscoverFunctionsManager: pull_functions exception {e}\n{traceback.format_exc()}")
                self.inited = True
        memory = self.load(function_input.api_key)
        response = []
        #loop = asyncio.get_event_loop()
        try:
            documents = await self.get_retrieved_nodes(memory,
                function_input.query, function_input.category, function_input.auth.namespace_id)
            if len(documents) > 0:
                parsed_response = self.extract_name_and_category(documents)
                response.append(parsed_response)
                # update last_accessed_at
                ids = [doc.metadata["id"] for doc in documents]
                for doc in documents:
                    doc.metadata.pop('relevance_score', None)
                await self.rate_limiter.execute(memory.base_retriever.vectorstore.aadd_documents, documents, ids=ids)
                #loop.run_in_executor(None, self.prune_functions)
        except Exception as e:
            logging.warn(f"DiscoverFunctionsManager: pull_functions exception {e}\n{traceback.format_exc()}")
        finally:
            end = time.time()
            logging.info(
                f"DiscoverFunctionsManager: pull_functions operation took {end - start} seconds")
            return response, end-start

    async def get_retrieved_nodes(self, memory: ContextualCompressionRetriever, query_str: str, category: str, namespace_id: str):
        kwargs = {}
        if len(category) > 0:
            kwargs["extra_index"] = category
        # if user provided then look for null or direct matches, otherwise look for null so it matches public functions
        if namespace_id:
            filter = rest.Filter(
                should=[
                    rest.FieldCondition(
                        key="metadata.namespace_id",
                        match=rest.MatchValue(value=namespace_id),
                    ),
                    rest.IsNullCondition(
                        is_null=rest.PayloadField(key="metadata.namespace_id")
                    )
                ]
            )
            kwargs["user_filter"] = filter
        else:
            filter = rest.Filter(
                should=[
                    rest.IsNullCondition(
                        is_null=rest.PayloadField(key="metadata.namespace_id")
                    )
                ]
            )
            kwargs["user_filter"] = filter
        return await memory.aget_relevant_documents(query_str, **kwargs)

    @cachetools.func.ttl_cache(maxsize=16384, ttl=36000)
    def load(self, api_key: str):
        """Load existing index data from the filesystem for a specific user."""
        start = time.time()
        memory = self.create_new_functions_retriever(api_key)
        end = time.time()
        logging.info(
            f"DiscoverFunctionsManager: Load operation took {end - start} seconds")
        return memory

    async def push_functions(self, namespace_id: str, api_key: str, functions):
        """Update the current index with new functions."""
        start = time.time()
        memory = self.load(api_key)
        try:
            logging.info("DiscoverFunctionsManager: adding functions to index...")

            function_types = ['information_retrieval',
                              'communication',
                              'data_processing',
                              'sensory_perception',
                              'programming']

            all_docs = []

            # Transform and concatenate function types
            for func_type in function_types:
                if func_type in functions:
                    transformed_functions = self.transform(
                        namespace_id, functions[func_type], func_type)
                    all_docs.extend(transformed_functions)
            ids = [doc.metadata["id"] for doc in all_docs]
            await self.rate_limiter.execute(memory.base_retriever.vectorstore.aadd_documents, all_docs, ids=ids)
        except Exception as e:
            logging.warn(f"DiscoverFunctionsManager: push_functions exception {e}\n{traceback.format_exc()}")
        finally:
            end = time.time()
            logging.info(
                f"DiscoverFunctionsManager: push_functions took {end - start} seconds")
            return "success", end-start

    def prune_functions(self):
        """Prune functions that haven't been used for atleast six weeks."""
        def attempt_prune():
            current_time = datetime.now()
            six_weeks_ago = current_time - timedelta(weeks=6)
            filter = rest.Filter(
                must=[
                    rest.FieldCondition(
                        key="metadata.last_accessed_at", 
                        range=rest.Range(lte=six_weeks_ago.timestamp()), 
                    )
                ]
            )
            self.client.delete(collection_name=self.collection_name, points_selector=filter)
        try:
            attempt_prune()
        except Exception as e:
            logging.warn(f"DiscoverFunctionsManager: prune_functions exception {e}\n{traceback.format_exc()}")
            # Attempt a second prune after reload
            try:
                attempt_prune()
            except Exception as e:
                # If prune after reload fails, propagate the error upwards
                logging.error(f"DiscoverFunctionsManager: prune_functions failed after reload, exception {e}\n{traceback.format_exc()}")
                raise
        return True