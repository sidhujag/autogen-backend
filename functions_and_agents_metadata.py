
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
from typing import List, Optional, Any, Tuple
class AuthAgent(BaseModel):
    api_key: str
    namespace_id: str
    def to_dict(self):
        return {"namespace_id": self.namespace_id}

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
    agents: Optional[dict[str, bool]] = None
    invitees: Optional[dict[str, bool]] = None

class BaseAgent(BaseModel):
    name: str = Field(default="")
    auth: AuthAgent
    description: str = Field(default="")
    default_auto_reply: str = Field(default="")
    system_message: str = Field(default="")
    system_message: str = Field(default="")
    category: str = Field(default="")
    agents: dict = Field(default_factory=dict)
    invitees: dict = Field(default_factory=dict)

class OpenAIParameter(BaseModel):
    type: str
    properties: dict[str, Any]
    required: Optional[List[str]] = None

class AddFunctionModel(BaseModel):
    name: str
    namespace_id: str = Field(default="")
    description: str
    parameters: OpenAIParameter
    category: str
    packages: List[str] = Field(default_factory=list)
    code: str = Field(default="")
    class_name: str = Field(default="")


class Agent(BaseAgent):
    functions: List[AddFunctionModel] = Field(default_factory=list)

class AgentModel(BaseAgent):
    function_names: List[str] = Field(default_factory=list)

class AddFunctionInput(BaseModel):
    name: str
    auth: AuthAgent
    description: str
    parameters: OpenAIParameter
    category: str
    packages: Optional[List[str]] = None
    code: Optional[str] = None
    class_name: Optional[str] = None
    def to_add_function_model_dict(self):
        data = self.dict(exclude={"auth"}, exclude_none=True)
        data.update(self.auth.to_dict())
        return data

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
            logging.warn(f"FunctionsAndAgentsMetadata: get_function exception {e}\n{traceback.format_exc()}")
            return False, end-start

    async def pull_functions(self, namespace_id: str, function_names: List[str]) -> List[AddFunctionModel]:
        start = time.time()
        if self.client is None or self.funcs_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            doc_cursor = self.sync_rate_limiter.execute(self.funcs_collection.find, {"name": {"$in": function_names}, "namespace_id": namespace_id})
            docs = await self.rate_limiter.execute(doc_cursor.to_list, length=1000)
            # Convert each document in docs to a dictionary if necessary, then to an AddFunctionModel
            function_models = [AddFunctionModel(**doc) for doc in docs if isinstance(doc, dict)]
            end = time.time()
            return function_models, end-start
        except Exception as e:
            end = time.time()
            logging.warn(f"FunctionsAndAgentsMetadata: get_function exception {e}\n{traceback.format_exc()}")
            return [], end-start

    async def set_functions(self, functions: List[AddFunctionInput]) -> Tuple[str, float]:
        start = time.time()
        if self.client is None or self.funcs_collection is None or self.rate_limiter is None:
            await self.initialize()
        upsert = False
        operations = []
        function_names = [func.name for func in functions]
        existing_functions_docs = await self.get_existing_functions(function_names, functions[0].auth.namespace_id)
        existing_functions = {func['name']: AddFunctionModel(**func) for func in existing_functions_docs}
        
        for function in functions:
            function_model_data = function.to_add_function_model_dict()
            function_model = AddFunctionModel(**function_model_data)
            
            if function.name in existing_functions:
                existing_function = existing_functions[function.name]
                if function_model != existing_function:  # Compare the entire objects
                    update_op = pymongo.UpdateOne(
                        {"name": function.name, "namespace_id": function.auth.namespace_id},
                        {"$set": function_model.dict()},
                        upsert=True
                    )
                    operations.append(update_op)
            else:
                update_op = pymongo.UpdateOne(
                    {"name": function.name, "namespace_id": function.auth.namespace_id},
                    {"$set": function_model.dict()},
                    upsert=True
                )
                operations.append(update_op)

        if operations:
            try:
                await self.rate_limiter.execute(
                    self.funcs_collection.bulk_write,
                    operations
                )
            except Exception as e:
                end = time.time()
                return f"FunctionsAndAgentsMetadata: set_functions exception {e}\n{traceback.format_exc()}", end - start
            
        end = time.time()
        return "success" if upsert else "Functions were identical, no changed fields found!", (end-start)


    async def get_existing_functions(self, function_names: List[str], namespace_id: str) -> List[dict]:
        query = {
            "name": {"$in": function_names},
            "namespace_id": namespace_id
        }
        cursor = self.funcs_collection.find(query)
        existing_functions = await cursor.to_list(length=None)
        return existing_functions

    async def get_agent(self, agent_input: GetAgentModel, resolve_functions: bool = True):
        start = time.time()
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            doc = await self.rate_limiter.execute(self.agents_collection.find_one, {"name": agent_input.name, "namespace_id": agent_input.auth.namespace_id})
            if doc is None:
                end = time.time()
                return AgentModel(auth=agent_input.auth), end-start
            doc['auth'] = AuthAgent(**doc['auth'])
            agent_model = AgentModel(**doc)
            if resolve_functions:
                agent = Agent(**agent_model.dict())
                agent.functions, elapsed = await self.pull_functions(agent_input.auth.namespace_id, agent_model.function_names)
                end = time.time()
                return agent, (end-start)
            else:
                end = time.time()
                return agent_model, end-start
        except Exception as e:
            end = time.time()
            logging.warn(f"FunctionsAndAgentsMetadata: get_agent exception {e}\n{traceback.format_exc()}")
            return AgentModel(auth=agent_input.auth), end-start
        
    def update_agent(self, agent, agent_upsert):
        changed = False
        # Convert agent and agent_upsert to dictionaries for easier field checking and updating
        agent_dict = agent.dict() if isinstance(agent, BaseModel) else agent
        agent_upsert_dict = agent_upsert.dict(exclude_none=True) if isinstance(agent_upsert, BaseModel) else agent_upsert

        for field, new_value in agent_upsert_dict.items():
            # Skip fields that don't exist on the agent object
            if field not in agent_dict:
                continue
            old_value = agent_dict.get(field, None)
            if field == 'function_names' and old_value is not None:
                if not isinstance(new_value, list):
                    new_value = [new_value]
                unique_new_values = [value for value in new_value if value not in old_value]
                if unique_new_values:
                    changed = True
                    agent_dict[field] = old_value + unique_new_values
            else:
                if new_value != old_value:
                    changed = True
                    agent_dict[field] = new_value
        # Convert agent_dict back to a Pydantic model if agent was initially a Pydantic model
        if isinstance(agent, BaseModel):
            agent = agent.__class__(**agent_dict)

        return changed, agent


    async def upsert_agent(self, agent_upsert: UpsertAgentInput):
        start = time.time()
        changed = False
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            agent, elapsed = await self.get_agent(GetAgentModel(name=agent_upsert.name, auth=agent_upsert.auth), False)
            # if found in DB we are updating
            if agent.name != "":
                # if it is not a global agent then only let the owner upsert it
                if agent.auth.namespace_id != "" and agent_upsert.auth.namespace_id != agent.auth.namespace_id:
                    end = time.time()
                    return "User cannot modify someone elses agent.", (end-start)
                changed, agent = self.update_agent(agent, agent_upsert)
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
            return f"FunctionsAndAgentsMetadata: upsert_agent exception {e}\n{traceback.format_exc()}", (end-start), None
        end = time.time()
        return "success" if changed else "Agent not changed, no changed fields found!", (end-start), agent

    async def delete_agent(self, agent_delete: DeleteAgentModel):
        start = time.time()
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()
        try:
            agent = await self.get_agent(GetAgentModel(name=agent_delete.name, auth=agent_delete.auth), False)
            # if found in DB we are updating
            if agent.name != "":
                # cannot delete global agent nor someone elses
                if agent.auth.namespace_id == "" or agent_delete.auth.namespace_id != agent.auth.namespace_id:
                    end = time.time()
                    return "User cannot delete someone elses agent.", (end-start)            
                delete_result = await self.rate_limiter.execute(
                    self.agents_collection.delete_one,
                    {"name": agent_delete.name, "namespace_id": agent_delete.auth.namespace_id}
                )
                if delete_result.delete_count == 0:
                    logging.warn("No documents were delete.")
        except Exception as e:
            end = time.time()
            return f"FunctionsAndAgentsMetadata: delete_agent exception {e}\n{traceback.format_exc()}", (end-start)
        end = time.time()
        return "success", (end-start)
