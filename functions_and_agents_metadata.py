
import logging
import os
import traceback
import time
import pymongo

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from pydantic import BaseModel
from rate_limiter import RateLimiter
from asyncio import Lock
from typing import List, Dict, Optional, Union

class GetAgentModel(BaseModel):
    name: str

class GetFunctionsModel(BaseModel):
    functions: List[str]

class UpsertAgentInput(BaseModel):
    name: str
    user_id: str
    api_key: str
    description: Optional[str] = None 
    system_message: Optional[str] = None
    function_names: Optional[List[str]] = None # cumulative
    category: Optional[str] = None
    agents: Optional[List[Dict]] = None
    invitees: Optional[List[str]] = None
    
class Agent(BaseModel):
    name: str = ""
    user_id: str = ""
    description: str = ""
    system_message: str = ""
    function_names: List[str] = []
    category: str = ""
    agents: List[Dict] = [] 
    invitees: List[str] = []

class AddFunctionInput(BaseModel):
    name: str
    user_id: str
    api_key: str
    description: str
    arguments: Dict[str, Union[str, Dict]] 
    required: Optional[List[str]] = None
    category: str
    packages: Optional[List[str]] = None
    code: Optional[str] = None
    class_name: Optional[str] = None
    
class AddFunctionModel(BaseModel):
    name: str
    user_id: str = ""
    description: str
    arguments: Dict[str, Union[str, Dict]] 
    required: List[str] = []
    category: str
    packages: List[str] = []
    code: str = ""
    class_name: str = ""

class FunctionsAndAgentsMetadata:
    def __init__(self):
        load_dotenv()  # Load environment variables
        mongopw = os.getenv("MONGODB_PW")
        self.uri = f"mongodb+srv://superdapp:{mongopw}@cluster0.qyi8mou.mongodb.net/?retryWrites=true&w=majority"
        self.client = None
        self.funcs_collection = None
        self.agents_collection = None
        self.rate_limiter = None
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
                await self.funcs_collection.create_index([("name", pymongo.ASCENDING), ("user_id", pymongo.ASCENDING)], unique=True)
                self.agents_collection = self.db['Agents']
                await self.agents_collection.create_index([("name", pymongo.ASCENDING), ("user_id", pymongo.ASCENDING)], unique=True)
                self.rate_limiter = RateLimiter(rate=10, period=1)
            except Exception as e:
                logging.warn(f"FunctionsAndAgentsMetadata: initialize exception {e}\n{traceback.format_exc()}")
    
    async def does_function_exist(self, user_id: str, function_name: str) -> bool:
        start = time.time()
        if self.client is None or self.funcs_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            doc = await self.rate_limiter.execute(self.funcs_collection.find_one, {"name": function_name, "user_id": user_id})
            end = time.time()
            return doc is not None, end-start
        except Exception as e:
            end = time.time()
            return f"FunctionsAndAgentsMetadata: get_function exception {e}\n{traceback.format_exc()}", end-start

    async def pull_function_names(self, user_id: str, function_names: List[str]) -> List[str]:
        start = time.time()
        if self.client is None or self.funcs_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            doc_cursor = await self.rate_limiter.execute(self.funcs_collection.find, {"name": {"$in": function_names}, "user_id": user_id})
            if doc_cursor is None:
                end = time.time()
                return None, end-start
            docs = await doc_cursor.to_list(length=1000)
            retrieved_function_names = [doc['name'] for doc in docs if 'name' in doc]
            end = time.time()
            return retrieved_function_names, end-start
        except Exception as e:
            end = time.time()
            return f"FunctionsAndAgentsMetadata: get_function exception {e}\n{traceback.format_exc()}", end-start

    async def set_function(self, function: AddFunctionInput):
        start = time.time()
        if self.client is None or self.funcs_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            db_function_exists = await self.does_function_exist(function.user_id, function.name)
            if db_function_exists is True:
                return "Function with that name already exists.", end-start
            update_result = await self.rate_limiter.execute(
                self.funcs_collection.update_one,
                {"name": function.name, "user_id": function.user_id},
                {"$set": AddFunctionModel(function).dict()},
                upsert=True
            )
            if update_result.matched_count == 0 and update_result.upserted_id is None:
                logging.warn("No documents were inserted or updated.")
        except Exception as e:
            end = time.time()
            return f"FunctionsAndAgentsMetadata: set_function exception {e}\n{traceback.format_exc()}", end-start
        end = time.time()
        return "success", end-start
    
    async def get_agent(self, user_id: str, agent_name: str, resolve_functions: bool = True) -> Agent:
        start = time.time()
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            doc = await self.rate_limiter.execute(self.agents_collection.find_one, {"name": agent_name, "user_id": user_id})
            if doc is None:
                end = time.time()
                return Agent(), end-start
            agent = Agent(**doc)
            if resolve_functions:
                agent.function_names = await self.pull_function_names(agent.function_names)
            end = time.time()
            return agent, end-start
        except Exception as e:
            end = time.time()
            return f"FunctionsAndAgentsMetadata: get_agent exception {e}\n{traceback.format_exc()}", end-start

    async def upsert_agent(self, agent_upsert: UpsertAgentInput):
        start = time.time()
        changed = False
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            agent: Agent = await self.get_agent(agent_upsert.user_id, agent_upsert.name)
            # if it is not a global agent then only let the owner upsert it
            if agent.user_id != "" and agent_upsert.user_id != agent.user_id:
                end = time.time()
                return "User cannot modify someone elses agent.", end-start
            if agent_upsert.function_names:
                changed = True
                agent.function_names.append(agent_upsert.function_names)
            if agent_upsert.system_message:
                changed = True
                agent.system_message = agent_upsert.system_message
            if agent_upsert.category:
                changed = True
                agent.category = agent_upsert.category
            if agent_upsert.description:
                changed = True
                agent.description = agent_upsert.description
            if agent_upsert.agents:
                changed = True
                agent.agents = agent_upsert.agents
            if agent_upsert.invitees:
                changed = True
                agent.invitees = agent_upsert.invitees
            if changed:
                update_result = await self.rate_limiter.execute(
                    self.agents_collection.update_one,
                    {"name": agent_upsert.name, "user_id": agent_upsert.user_id},
                    {"$set": agent.dict()},
                    upsert=True
                )
                if update_result.matched_count == 0 and update_result.upserted_id is None:
                    logging.warn("No documents were inserted or updated.")
        except Exception as e:
            end = time.time()
            return f"FunctionsAndAgentsMetadata: set_agent exception {e}\n{traceback.format_exc()}", end-start
        end = time.time()
        return "success" if changed else "Agent not changed, no changed fields found!", end-start
