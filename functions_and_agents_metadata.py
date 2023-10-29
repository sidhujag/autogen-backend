
import logging
import os
import traceback
import time
import pymongo

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCursor
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from rate_limiter import RateLimiter, SyncRateLimiter
from asyncio import Lock
from typing import List, Dict, Optional, Union
class AuthAgent:
    api_key: str
    namespace_id: str
    def __str__(self):
        return self.api_key + self.namespace_id

    def __eq__(self,other):
        return self.api_key == other.api_key and self.namespace_id == other.namespace_id

    def __hash__(self):
        return hash(str(self))


class DeleteAgentModel(BaseModel):
    name: str
    auth: AuthAgent
    
class GetAgentModel(BaseModel):
    name: str
    auth: AuthAgent

class UpsertAgentInput(BaseModel):
    name: str
    auth: AuthAgent
    human_input_mode: Optional[str] = None
    default_auto_reply: Optional[str] = None
    description: Optional[str] = None 
    system_message: Optional[str] = None
    function_names: Optional[List[str]] = None
    category: Optional[str] = None
    agents: Optional[List[str]] = None
    invitees: Optional[List[str]] = None

class BaseAgent(BaseModel):
    name: str = Field(default="")
    namespace_id: str = Field(default="")
    description: str = Field(default="")
    default_auto_reply: str = Field(default="")
    system_message: str = Field(default="")
    system_message: str = Field(default="")
    category: str = Field(default="")
    agents: List[str] = Field(default_factory=list)
    invitees: List[str] = Field(default_factory=list)

class Agent(BaseAgent):
    functions: List[Dict] = Field(default_factory=list)

class AgentModel(BaseAgent):
    function_names: List[str] = Field(default_factory=list)
    
class AddFunctionInput(BaseModel):
    name: str
    auth: AuthAgent
    description: str
    arguments: Optional[Dict[str, Union[str, Dict]]] = None
    required: Optional[List[str]] = None
    category: str
    packages: Optional[List[str]] = None
    code: Optional[str] = None
    class_name: Optional[str] = None
    
class AddFunctionModel(BaseModel):
    name: str
    namespace_id: str = Field(default="")
    description: str
    arguments: Dict[str, Union[str, Dict]] = Field(default_factory=Dict)
    required: List[str] = Field(default_factory=list)
    category: str
    packages: List[str] = Field(default_factory=list)
    code: str = Field(default="")
    class_name: str = Field(default="")

class FunctionsAndAgentsMetadata:
    def __init__(self):
        load_dotenv()  # Load environment variables
        mongopw = os.getenv("MONGODB_PW")
        self.uri = f"mongodb+srv://superdapp:{mongopw}@cluster0.qyi8mou.mongodb.net/?retryWrites=true&w=majority"
        self.client = None
        self.funcs_collection = None
        self.agents_collection = None
        self.rate_limiter = None
        self.sync_rate_limiter = None
        self.init_lock = Lock()

    async def initialize(self):
        async with self.init_lock:
            if self.client is not None:
                return
            try:
                self.client = AsyncIOMotorClient(self.uri, server_api=ServerApi('1'))
                await self.client.admin.command('ping')
                print("Pinged your deployment. You successfully connected to MongoDB!")
                
                # Setup references after successful connection
                self.db = self.client['FunctionsAndAgentsDB']
                self.funcs_collection = self.db['Functions']
                await self.funcs_collection.create_index([("name", pymongo.ASCENDING), ("namespace_id", pymongo.ASCENDING)], unique=True)
                self.agents_collection = self.db['Agents']
                await self.agents_collection.create_index([("name", pymongo.ASCENDING), ("namespace_id", pymongo.ASCENDING)], unique=True)
                self.rate_limiter = RateLimiter(rate=10, period=1)
                self.sync_rate_limiter = SyncRateLimiter(rate=10, period=1)
                
            except Exception as e:
                logging.warn(f"FunctionsAndAgentsMetadata: initialize exception {e}\n{traceback.format_exc()}")
    
    async def does_function_exist(self, namespace_id: str, function_name: str) -> bool:
        start = time.time()
        if self.client is None or self.funcs_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            doc = await self.rate_limiter.execute(self.funcs_collection.find_one, {"name": function_name, "namespace_id": namespace_id})
            end = time.time()
            return doc is not None, end-start
        except Exception as e:
            end = time.time()
            return f"FunctionsAndAgentsMetadata: get_function exception {e}\n{traceback.format_exc()}", end-start

    async def pull_functions(self, namespace_id: str, function_names: List[str]) -> List[AddFunctionModel]:
        start = time.time()
        if self.client is None or self.funcs_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            doc_cursor = self.sync_rate_limiter.execute(self.funcs_collection.find, {"name": {"$in": function_names}, "namespace_id": namespace_id})
            docs = await self.rate_limiter.execute(doc_cursor.to_list, length=1000)
            function_models = [AddFunctionModel(**doc) for doc in docs]
            end = time.time()
            return function_models, end-start
        except Exception as e:
            end = time.time()
            logging.warn(f"FunctionsAndAgentsMetadata: get_function exception {e}\n{traceback.format_exc()}")
            return [], end-start

    async def set_function(self, function: AddFunctionInput):
        start = time.time()
        if self.client is None or self.funcs_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            db_function_exists = await self.does_function_exist(function.auth.namespace_id, function.name)
            if db_function_exists is True:
                return "Function with that name already exists.", end-start
            update_result = await self.rate_limiter.execute(
                self.funcs_collection.update_one,
                {"name": function.name, "namespace_id": function.auth.namespace_id},
                {"$set": AddFunctionModel(**function.dict(exclude_none=True)).dict()},
                upsert=True
            )
            if update_result.matched_count == 0 and update_result.upserted_id is None:
                logging.warn("No documents were inserted or updated.")
        except Exception as e:
            end = time.time()
            return f"FunctionsAndAgentsMetadata: set_function exception {e}\n{traceback.format_exc()}", end-start
        end = time.time()
        return "success", end-start
    
    async def get_agent(self, agent_input: GetAgentModel, resolve_functions: bool = True):
        start = time.time()
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            doc = await self.rate_limiter.execute(self.agents_collection.find_one, {"name": agent_input.name, "namespace_id": agent_input.auth.namespace_id})
            if doc is None:
                end = time.time()
                return AgentModel(), end-start
            agent_model = AgentModel(**doc)
            if resolve_functions:
                agent = Agent(**agent_model.dict())
                agent.functions, elapsed = await self.pull_functions(agent_input.auth.namespace_id, agent_model.function_names)
                end = time.time()
                return agent, (end-start) + elapsed
            else:
                end = time.time()
                return agent_model, end-start
        except Exception as e:
            end = time.time()
            logging.warn(f"FunctionsAndAgentsMetadata: get_agent exception {e}\n{traceback.format_exc()}")
            return AgentModel(), end-start
        
    def update_agent(self, agent, agent_upsert):
        changed = False
        for field, new_value in agent_upsert.dict(exclude_none=True).items():
            # Skip fields that don't exist on the agent object
            if not hasattr(agent, field):
                continue
            old_value = getattr(agent, field, None)
            if new_value != old_value:
                changed = True
                if field == 'function_names' and old_value is not None:
                    # Ensure both old_value and new_value are lists before appending
                    new_value = old_value + (new_value if isinstance(new_value, list) else [new_value])
                setattr(agent, field, new_value)  # replace old value or append to it
        return changed

    async def upsert_agent(self, agent_upsert: UpsertAgentInput):
        start = time.time()
        changed = False
        elapsed = 0
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            agent, elapsed = await self.get_agent(GetAgentModel(name=agent_upsert.name, namespace_id=agent_upsert.auth.namespace_id), False)
            # if found in DB we are updating
            if agent.name != "":
                # if it is not a global agent then only let the owner upsert it
                if agent.namespace_id != "" and agent_upsert.auth.namespace_id != agent.namespace_id:
                    end = time.time()
                    return "User cannot modify someone elses agent.", (end-start) + elapsed
                changed = self.update_agent(agent, agent_upsert)
            # otherwise new agent
            else:
                agent = AgentModel(**agent_upsert.dict(exclude_none=True))
                changed = True
            if changed:
                update_result = await self.rate_limiter.execute(
                    self.agents_collection.update_one,
                    {"name": agent_upsert.name, "namespace_id": agent_upsert.auth.namespace_id},
                    {"$set": agent.dict()},
                    upsert=True
                )
                if update_result.matched_count == 0 and update_result.upserted_id is None:
                    logging.warn("No documents were inserted or updated.")
        except Exception as e:
            end = time.time()
            return f"FunctionsAndAgentsMetadata: set_agent exception {e}\n{traceback.format_exc()}", (end-start) + elapsed
        end = time.time()
        return "success" if changed else "Agent not changed, no changed fields found!", (end-start) + elapsed

    async def delete_agent(self, agent_delete: DeleteAgentModel):
        start = time.time()
        elapsed = 0
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            agent, elapsed = await self.get_agent(GetAgentModel(name=agent_delete.name, namespace_id=agent_delete.auth.namespace_id), False)
            # if found in DB we are updating
            if agent.name != "":
                # cannot delete global agent nor someone elses
                if agent.namespace_id == "" or agent_delete.auth.namespace_id != agent.namespace_id:
                    end = time.time()
                    return "User cannot delete someone elses agent.", (end-start) + elapsed            
                delete_result = await self.rate_limiter.execute(
                    self.agents_collection.delete_one,
                    {"name": agent_delete.name, "namespace_id": agent_delete.auth.namespace_id}
                )
                if delete_result.delete_count == 0:
                    logging.warn("No documents were delete.")
        except Exception as e:
            end = time.time()
            return f"FunctionsAndAgentsMetadata: delete_agent exception {e}\n{traceback.format_exc()}", (end-start) + elapsed
        end = time.time()
        return "success", (end-start) + elapsed
