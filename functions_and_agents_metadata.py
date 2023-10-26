
import logging
import os
import traceback
import time

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

class UpsertAgentModel(BaseModel):
    name: str
    description: Optional[str] = None 
    system_message: Optional[str] = None
    function_names: Optional[List[str]] = None # cumulative
    category: Optional[str] = None
    agents: Optional[List[Dict]] = None
    invitees: Optional[List[str]] = None
    
class Agent(BaseModel):
    name: str = ""
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
    description: str
    arguments: Dict[str, Union[str, Dict]] 
    required: Optional[List[str]] = None
    category: str
    packages: Optional[List[str]] = None
    code: Optional[str] = None
    class_name: Optional[str] = None

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
                self.agents_collection = self.db['Agents']
                self.rate_limiter = RateLimiter(rate=10, period=1)
            except Exception as e:
                logging.warn(f"FunctionsAndAgentsMetadata: initialize exception {e}\n{traceback.format_exc()}")
    
    async def pull_functions(self, functions: List[str]):
        start = time.time()
        if self.client is None or self.funcs_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            doc = await self.rate_limiter.execute(self.funcs_collection.find_one, {"_id": functions})
            if doc is None:
                end = time.time()
                return "function not found", end-start
            end = time.time()
            return doc["function"], end-start
        except Exception as e:
            end = time.time()
            return f"FunctionsAndAgentsMetadata: get_function exception {e}\n{traceback.format_exc()}", end-start

    async def set_function(self, name, function: AddFunctionModel):
        start = time.time()
        if self.client is None or self.funcs_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            function.class_name = function.class_name if function.class_name else ""
            function.code = function.code if function.code else ""
            function.packages = function.packages if function.packages else []
            function.required = function.required if function.required else []
            functionObj = {"_id": name, "function": function}
            update_result = await self.rate_limiter.execute(self.role_collection.update_one, {"_id": name}, {"$set": functionObj}, upsert=True)
            if update_result.matched_count == 0 and update_result.upserted_id is None:
                logging.warn("No documents were inserted or updated.")
        except Exception as e:
            end = time.time()
            return f"FunctionsAndAgentsMetadata: set_function exception {e}\n{traceback.format_exc()}", end-start
        end = time.time()
        return "success", end-start
    
    async def get_agent(self, agent_name: str, resolve_functions: bool = True) -> Agent:
        start = time.time()
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            doc = await self.rate_limiter.execute(self.agents_collection.find_one, {"_id": agent_name})
            if doc is None:
                end = time.time()
                return Agent(), end-start
            agent = doc["agent"]
            if resolve_functions:
                agent["functions"] = await self.pull_functions(agent["functions"])
            end = time.time()
            return Agent(agent), end-start
        except Exception as e:
            end = time.time()
            return f"FunctionsAndAgentsMetadata: get_agent exception {e}\n{traceback.format_exc()}", end-start


    async def upsert_agent(self, agent_upsert: UpsertAgentModel):
        start = time.time()
        changed = False
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            agent = await self.get_agent(agent.name)
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
                agentObj = {"_id": agent.name, "agent": agent}
                update_result = await self.rate_limiter.execute(self.agents_collection.update_one, {"_id": agent.name}, {"$set": agentObj}, upsert=True)
                if update_result.matched_count == 0 and update_result.upserted_id is None:
                    logging.warn("No documents were inserted or updated.")
        except Exception as e:
            end = time.time()
            return f"FunctionsAndAgentsMetadata: set_agent exception {e}\n{traceback.format_exc()}", end-start
        end = time.time()
        return "success" if changed else "Agent not changed, no changed fields found!", end-start
