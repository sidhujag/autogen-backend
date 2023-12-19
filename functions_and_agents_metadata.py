
import logging
import os
import traceback
import pymongo
import json

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.server_api import ServerApi
from pymongo.errors import PyMongoError
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from rate_limiter import RateLimiter
from asyncio import Lock
from typing import List, Optional, Any, Tuple, Dict

class AuthAgent(BaseModel):
    api_key: str = ''
    zapier_api_key: str = ''
    gh_pat: str = ''
    gh_user: str = ''
    namespace_id: str = ''
    def to_dict(self):
        return {"namespace_id": self.namespace_id}

    def __init__(self, namespace_id: str, **data):
        if 'api_key' not in data:
            data['api_key'] = ''
        super().__init__(namespace_id=namespace_id, **data)

class DeleteAgentModel(BaseModel):
    name: str
    auth: AuthAgent
    
class DeleteCodeAssistantsModel(BaseModel):
    name: str
    auth: AuthAgent

class DeleteCodeRepositoryModel(BaseModel):
    name: str
    auth: AuthAgent
    
class DeleteGroupsModel(BaseModel):
    name: str
    auth: AuthAgent

class GetAgentModel(BaseModel):
    name: str
    auth: AuthAgent

class GetGroupModel(BaseModel):
    name: str
    auth: AuthAgent

class GetFunctionModel(BaseModel):
    name: str
    auth: AuthAgent

class GetCodingAssistantsModel(BaseModel):
    name: str
    auth: AuthAgent

class GetCodeRepositoryModel(BaseModel):
    name: str
    auth: AuthAgent
    
class UpsertAgentModel(BaseModel):
    name: str
    auth: AuthAgent
    assistant_id: Optional[str] = None
    human_input_mode: Optional[str] = None
    default_auto_reply: Optional[str] = None
    description: Optional[str] = None
    system_message: Optional[str] = None
    functions_to_add: Optional[List[str]] = None
    functions_to_remove: Optional[List[str]] = None
    category: Optional[str] = None
    capability: Optional[int] = None
    files_to_add: Optional[Dict[str, str]] = None
    files_to_remove: Optional[List[str]] = None
    def exclude_auth_dict(self):
        data = self.dict(exclude_none=True, exclude={'functions_to_add', 'functions_to_remove', 'files_to_add', 'files_to_remove', 'auth'})
        data.update(self.auth.to_dict())
        return data

class UpsertGroupInput(BaseModel):
    name: str
    auth: AuthAgent
    description: Optional[str] = None
    agents_to_add: Optional[List[str]] = None
    agents_to_remove: Optional[List[str]] = None
    locked: Optional[bool] = None
    def exclude_auth_dict(self):
        data = self.dict(exclude={"auth"}, exclude_none=True)
        data.update(self.auth.to_dict())
        return data

class UpsertCodingAssistantInput(BaseModel):
    name: str
    repository_name: str
    auth: AuthAgent
    description: Optional[str] = None
    model: Optional[str] = None
    files: Optional[List[str]] = None
    show_diffs: Optional[bool] = None
    dry_run: Optional[bool] = None
    map_tokens: Optional[int] = None
    verbose: Optional[bool] = None
    def exclude_auth_dict(self):
        data = self.dict(exclude={"auth"}, exclude_none=True)
        data.update(self.auth.to_dict())
        return data

class UpsertCodeRepositoryInput(BaseModel):
    name: str
    auth: AuthAgent
    description: Optional[str] = None
    private: Optional[bool] = None
    gh_remote_url: Optional[str] = None
    upstream_gh_remote_url: Optional[str] = None
    associated_code_assistants: Optional[set[str]] = None
    def exclude_auth_dict(self):
        data = self.dict(exclude={"auth"}, exclude_none=True)
        data.update(self.auth.to_dict())
        return data
    
class AgentStats(BaseModel):
    count: int
    description: str

class BaseAgent(BaseModel):
    name: str = Field(default="")
    auth: AuthAgent
    assistant_id: str = Field(default="")
    human_input_mode: str = Field(default="")
    default_auto_reply: str = Field(default="")
    description: str = Field(default="")
    system_message: str = Field(default="")
    category: str = Field(default="")
    capability: int = Field(default=0)
    files: Dict[str, str] = Field(default_factory=dict)
    function_names: List[str] = Field(default_factory=list)
    def __init__(self, **data):
        if 'auth' not in data and 'namespace_id' in data:
            data['auth'] = {'namespace_id': data['namespace_id']}
        elif 'auth' in data and isinstance(data['auth'], dict) and 'namespace_id' in data:
            data['auth']['namespace_id'] = data['namespace_id']
        super().__init__(**data)
        
class BaseGroup(BaseModel):
    name: str = Field(default="")
    auth: AuthAgent
    description: str = Field(default="")
    agent_names: List[str] = Field(default_factory=list)
    outgoing: Dict[str, int] = Field(default_factory=dict)
    incoming: Dict[str, int] = Field(default_factory=dict)
    locked: Optional[bool] = Field(default=False)
    def __init__(self, **data):
        if 'auth' not in data and 'namespace_id' in data:
            data['auth'] = {'namespace_id': data['namespace_id']}
        elif 'auth' in data and isinstance(data['auth'], dict) and 'namespace_id' in data:
            data['auth']['namespace_id'] = data['namespace_id']
        super().__init__(**data)
        
class OpenAIParameter(BaseModel):
    type: str = "object"
    properties: dict = Field(default_factory=dict)
    required: List[str] = Field(default_factory=list)

class BaseFunction(BaseModel):
    name: str
    status: str
    last_updater: str
    description: str
    parameters: OpenAIParameter = Field(default_factory=OpenAIParameter)
    category: str
    function_code: str = Field(default="")
    class_name: str = Field(default="")

class BaseCodingAssistant(BaseModel):
    name: str = Field(default="")
    auth: AuthAgent
    repository_name: str = Field(default="")
    description: str = Field(default="")
    model: str = Field(default="")
    files: List[str] = Field(default=[])
    show_diffs: bool = Field(default=False)
    dry_run: bool = Field(default=False)
    map_tokens: int = Field(default=1024)
    verbose: bool = Field(default=False)
    def __init__(self, **data): 
        if 'auth' not in data and 'namespace_id' in data:
            data['auth'] = {'namespace_id': data['namespace_id']}
        elif 'auth' in data and isinstance(data['auth'], dict) and 'namespace_id' in data:
            data['auth']['namespace_id'] = data['namespace_id']
        super().__init__(**data)

class BaseCodeRepository(BaseModel):
    name: str = Field(default="")
    auth: AuthAgent
    description: str = Field(default="")
    gh_remote_url: str = Field(default="")
    upstream_gh_remote_url: str = Field(default="")
    associated_code_assistants: set[str] = Field(default=set())
    private: bool = Field(default=False)
    def __init__(self, **data): 
        if 'auth' not in data and 'namespace_id' in data:
            data['auth'] = {'namespace_id': data['namespace_id']}
        elif 'auth' in data and isinstance(data['auth'], dict) and 'namespace_id' in data:
            data['auth']['namespace_id'] = data['namespace_id']
        super().__init__(**data)
        
class AddFunctionModel(BaseFunction):
    namespace_id: str = Field(default="")

class AddFunctionInput(BaseModel):
    name: str
    auth: AuthAgent
    last_updater: str
    status: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    class_name: str = None
    parameters: OpenAIParameter = OpenAIParameter(type="object", properties={})
    function_code: Optional[str] = None
    def exclude_auth_dict(self):
        data = self.dict(exclude={"auth"}, exclude_none=True)
        data.update(self.auth.to_dict())
        return data
    
class UpdateComms(BaseModel):
    auth: AuthAgent
    sender: str
    receiver: str

class FunctionsAndAgentsMetadata:
    def __init__(self):
        load_dotenv()  # Load environment variables
        mongopw = os.getenv("MONGODB_PW")
        self.uri = f"mongodb+srv://superdapp:{mongopw}@cluster0.qyi8mou.mongodb.net/?retryWrites=true&w=majority"
        self.client = None
        self.funcs_collection = None
        self.agents_collection = None
        self.groups_collection = None
        self.coding_assistants_collection = None
        self.code_repository_collection = None
        self.db = None
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
                await self.funcs_collection.create_index([("name", pymongo.ASCENDING), ("namespace_id", pymongo.ASCENDING)], unique=True)
                self.agents_collection = self.db['Agents']
                await self.agents_collection.create_index([("name", pymongo.ASCENDING), ("namespace_id", pymongo.ASCENDING)], unique=True)
                self.groups_collection = self.db['Groups']
                await self.groups_collection.create_index([("name", pymongo.ASCENDING), ("namespace_id", pymongo.ASCENDING)], unique=True)
                self.coding_assistants_collection = self.db['CodingAssistants']
                await self.coding_assistants_collection.create_index([("name", pymongo.ASCENDING), ("namespace_id", pymongo.ASCENDING)], unique=True)
                self.code_repository_collection = self.db['CodeRepository']
                await self.code_repository_collection.create_index([("name", pymongo.ASCENDING), ("namespace_id", pymongo.ASCENDING)], unique=True)
                self.rate_limiter = RateLimiter(rate=10, period=1)
                
            except Exception as e:
                logging.warning(f"FunctionsAndAgentsMetadata: initialize exception {e}\n{traceback.format_exc()}")

    async def do_functions_exist(self, namespace_id: str, function_names: List[str]) -> bool:
        if self.client is None or self.funcs_collection is None or self.rate_limiter is None:
            await self.initialize()

        try:
            query = {"name": {"$in": function_names}, "namespace_id": namespace_id}
            count = await self.funcs_collection.count_documents(query)
            return count == len(function_names)
        except Exception as e:
            logging.warning(f"FunctionsAndAgentsMetadata: do_functions_exist exception {e}\n{traceback.format_exc()}")
            return False

    async def do_agents_exist(self, namespace_id: str, agent_names: List[str]) -> bool:
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()

        try:
            query = {"name": {"$in": agent_names}, "namespace_id": namespace_id}
            count = await self.agents_collection.count_documents(query)
            return count == len(agent_names)
        except Exception as e:
            logging.warning(f"FunctionsAndAgentsMetadata: do_agents_exist exception {e}\n{traceback.format_exc()}")
            return False

    async def get_functions(self, function_inputs: List[GetFunctionModel]) -> List[BaseFunction]:
        if self.client is None or self.funcs_collection is None or self.rate_limiter is None:
            await self.initialize()

        try:
            unique_functions = {(function_input.name, function_input.auth.namespace_id) for function_input in function_inputs}
            names, namespace_ids = zip(*unique_functions)
            query = {"name": {"$in": names}, "namespace_id": {"$in": namespace_ids}}
            doc_cursor = self.funcs_collection.find(query)
            docs = await self.rate_limiter.execute(doc_cursor.to_list, length=None)
            function_models = [BaseFunction(**doc) for doc in docs if isinstance(doc, dict)] 
            return function_models
        except Exception as e:
            logging.warning(f"FunctionsAndAgentsMetadata: get_functions exception {e}\n{traceback.format_exc()}")
            return []

    async def get_agents(self, agent_inputs: List[GetAgentModel], resolve_functions: bool = True) -> Tuple[List[BaseAgent], str]:
        if not agent_inputs:
            return [], json.dumps({"error": "Agent input list is empty"})
        
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()

        try:
            unique_agents = {(agent_input.name, agent_input.auth.namespace_id) for agent_input in agent_inputs}
            names, namespace_ids = zip(*unique_agents)
            query = {"name": {"$in": names}, "namespace_id": {"$in": namespace_ids}}
            doc_cursor = self.agents_collection.find(query)
            agents_docs = await self.rate_limiter.execute(doc_cursor.to_list, length=None)
            agents = [BaseAgent(**doc) for doc in agents_docs]
            return agents, None
        except PyMongoError as e:
            logging.warning(f"FunctionsAndAgentsMetadata: get_agents exception {e}\n{traceback.format_exc()}")
            return [], json.dumps({"error": f"MongoDB error occurred while retrieving agents: {str(e)}"})
        except Exception as e:
            logging.warning(f"FunctionsAndAgentsMetadata: get_agents exception {e}\n{traceback.format_exc()}")
            return [], json.dumps({"error": f"Error occurred while retrieving agents: {str(e)}"})

    async def update_comms(self, agent_input: UpdateComms):
        if self.client is None or self.groups_collection is None or self.rate_limiter is None:
            await self.initialize()

        try:
            # Prepare the updates for outgoing and incoming message counts
            sender_outgoing_count_field = f"outgoing.{agent_input.receiver}"
            receiver_incoming_count_field = f"incoming.{agent_input.sender}"

            # Update the outgoing count for the sender
            sender_update = pymongo.UpdateOne(
                {"name": agent_input.sender, "namespace_id": agent_input.auth.namespace_id},
                {
                    "$inc": {sender_outgoing_count_field: 1},
                    "$setOnInsert": {"name": agent_input.sender, "namespace_id": agent_input.auth.namespace_id}
                },
                upsert=True
            )
            
            # Update the incoming count for the receiver
            receiver_update = pymongo.UpdateOne(
                {"name": agent_input.receiver, "namespace_id": agent_input.auth.namespace_id},
                {
                    "$inc": {receiver_incoming_count_field: 1},
                    "$setOnInsert": {"name": agent_input.receiver, "namespace_id": agent_input.auth.namespace_id}
                },
                upsert=True
            )

            operations = [sender_update, receiver_update]
            if operations:
                await self.groups_collection.bulk_write(operations)
            return "success"
        except PyMongoError as e:
            logging.warning(f"update_comms exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": f"MongoDB error occurred: {str(e)}"})


    async def upsert_functions(self, functions: List[AddFunctionInput]) -> str:
        if self.client is None or self.funcs_collection is None or self.rate_limiter is None:
            await self.initialize()

        operations = []
        try:
            for function in functions:
                # Check if the function exists
                existing_function = await self.funcs_collection.find_one(
                    {"name": function.name, "namespace_id": function.auth.namespace_id}
                )
                if not existing_function and function.category is None:
                    return json.dumps({"error": "New functions must have a category defined."})
                if not existing_function and function.status is None:
                    return json.dumps({"error": "New functions must have a status defined."})
                if not existing_function and function.status == "accepted" and function.function_code:
                    return json.dumps({"error": "New untested functions cannot have an accepted status."})
                if not existing_function and function.description is None:
                    return json.dumps({"error": "New functions must have a description defined."})
                if not existing_function and function.class_name is None and function.function_code is None:
                    return json.dumps({"error": "New functions must have either function_code or class_name defined."})
                if existing_function and function.function_code:
                    existing_function_model = AddFunctionModel(**existing_function)
                    # if status is changing to accepted make sure this updater is not the same as the last one
                    if function.status == "accepted" and existing_function_model.status != "accepted" and existing_function_model.last_updater == function.last_updater:
                        return json.dumps({"error": "A different agent must accept the function from the one that last updated the code."})
                    # if function already accepted then you must change state back to testing or development if you are updating code
                    elif existing_function_model.status == "accepted" and not function.status and function.function_code:
                        return json.dumps({"error": "Currently accepted function must change status (to either development or testing) if you are updating code."})
                    
                function_model_data = function.exclude_auth_dict()
                function_model = AddFunctionModel(**function_model_data)
                update_op = pymongo.UpdateOne(
                    {"name": function.name, "namespace_id": function.auth.namespace_id},
                    {"$set": function_model.dict()},
                    upsert=True
                )
                operations.append(update_op)

            if operations:   
                result = await self.rate_limiter.execute(
                    self.funcs_collection.bulk_write,
                    operations
                )

                # Check if anything was actually modified or upserted
                if result.modified_count + result.upserted_count == 0:
                    return json.dumps({"error": "No functions were upserted, no changes found!"})
                return "success"
        except PyMongoError as e:
            logging.warning(f"FunctionsAndAgentsMetadata: upsert_functions exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": f"MongoDB error occurred while upserting functions: {str(e)}"})
        except Exception as e:
            return json.dumps({"error": str(e)})

    async def upsert_agents(self, agents_upsert: List[UpsertAgentModel]) -> str:
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()

        operations = []
        try:
            for agent_upsert in agents_upsert:
                # Check if the agent exists
                existing_agent = await self.agents_collection.find_one(
                    {"name": agent_upsert.name, "namespace_id": agent_upsert.auth.namespace_id}
                )

                # If it's a new agent and no category is provided, fail the operation
                if not existing_agent and agent_upsert.category is None:
                    return json.dumps({"error": "New agents must have a category defined."})
                if agent_upsert.functions_to_add:
                    if not await self.do_functions_exist(agent_upsert.auth.namespace_id, agent_upsert.functions_to_add):
                        liststr = ", ".join(agent_upsert.functions_to_add)
                        return json.dumps({"error": f"One of the functions you are trying to add does not exist from list: {liststr}"})
                # Generate the update dictionary using Pydantic's .dict() method
                update_data = agent_upsert.exclude_auth_dict()
                # Create the update operation for the agent
                update = {"$set": update_data}

                # Initialize the $addToSet and $pull operations if they have not been initialized yet
                if agent_upsert.functions_to_add:
                    update["$addToSet"] = {"function_names": {"$each": agent_upsert.functions_to_add}}

                if agent_upsert.functions_to_remove:
                    update["$pull"] = {"function_names": {"$in": agent_upsert.functions_to_remove}}

                # Add files if provided
                if agent_upsert.files_to_add:
                    # Here we are assuming the update operation can handle a dictionary directly
                    # If not, you would need to adjust the logic to work with your specific MongoDB schema
                    for file_id, file_description in agent_upsert.files_to_add.items():
                        update["$set"][f"files.{file_id}"] = file_description

                # Remove files if provided
                if agent_upsert.files_to_remove:
                    # For removal, we use $unset since we are working with dictionary keys
                    update["$unset"] = {f"files.{file_id}": "" for file_id in agent_upsert.files_to_remove}
                    
                update_op = pymongo.UpdateOne(
                    {"name": agent_upsert.name, "namespace_id": agent_upsert.auth.namespace_id},
                    update,
                    upsert=True
                )
                operations.append(update_op)

            if operations:
                result = await self.rate_limiter.execute(
                    self.agents_collection.bulk_write,
                    operations
                )
                # Check if anything was actually modified or upserted
                if result.modified_count + result.upserted_count == 0:
                    return json.dumps({"error": "No agents were upserted, no changes found!"})
                return "success"
            else:
                return "No agents were provided."
        except PyMongoError as e:
            logging.warning(f"FunctionsAndAgentsMetadata: upsert_agents exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": f"MongoDB error occurred while upserting agents: {str(e)}"})
        except Exception as e:
            logging.warning(f"FunctionsAndAgentsMetadata: upsert_agents exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": str(e)})

    async def upsert_groups(self, groups_upsert: List[UpsertGroupInput]) -> str:
        if self.client is None or self.groups_collection is None or self.rate_limiter is None:
            await self.initialize()

        operations = []
        try:
            for group_upsert in groups_upsert:
                if group_upsert.agents_to_add:
                    if not await self.do_agents_exist(group_upsert.auth.namespace_id, group_upsert.agents_to_add):
                        return json.dumps({"error": "One of the agents you are trying to add does not exist"})

                query = {
                    "name": group_upsert.name, 
                    "namespace_id": group_upsert.auth.namespace_id
                }
                update = {
                    "$set": {k: v for k, v in group_upsert.exclude_auth_dict().items() if k not in ['agents_to_add', 'agents_to_remove']},
                    "$addToSet": {
                        "agent_names": {"$each": group_upsert.agents_to_add} if group_upsert.agents_to_add else None
                    },
                    "$pull": {
                        "agent_names": {"$in": group_upsert.agents_to_remove} if group_upsert.agents_to_remove else None
                    }
                }
                # Clean up the update dict to remove keys with `None` values
                update["$addToSet"] = {k: v for k, v in update.get("$addToSet", {}).items() if v is not None}
                update["$pull"] = {k: v for k, v in update.get("$pull", {}).items() if v is not None}
                
                # If after cleaning, the dictionaries are empty, remove them from the update
                if not update["$addToSet"]:
                    del update["$addToSet"]
                if not update["$pull"]:
                    del update["$pull"]
                update_op = pymongo.UpdateOne(query, update, upsert=True)
                operations.append(update_op)

            if operations:
                result = await self.rate_limiter.execute(
                    self.groups_collection.bulk_write,
                    operations
                )
                # Check if anything was actually modified or upserted
                if result.modified_count + result.upserted_count == 0:
                    return json.dumps({"error": "No groups were upserted, no changes found!"})
                return "success"
            else:
                return json.dumps({"error": "No groups were provided."})
        except PyMongoError as e:
            logging.warning(f"FunctionsAndAgentsMetadata: upsert_groups exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": f"MongoDB error occurred while upsert_groups agents: {str(e)}"})
        except Exception as e:
            logging.warning(f"FunctionsAndAgentsMetadata: upsert_groups exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": str(e)})

    async def upsert_coding_assistants(self, coding_assistants_upsert: List[UpsertCodingAssistantInput]) -> str:
        if self.client is None or self.coding_assistants_collection is None or self.rate_limiter is None:
            await self.initialize()

        operations = []
        try:
            for coding_assistant_upsert in coding_assistants_upsert:
                # Check if the assistant exists
                existing_assistant = await self.coding_assistants_collection.find_one(
                    {"name": coding_assistant_upsert.name, "namespace_id": coding_assistant_upsert.auth.namespace_id}
                )

                if not existing_assistant:
                    if existing_assistant.description is None:
                        return json.dumps({"error": "New coding assistant must have a description."})
                    if existing_assistant.repository_name is None:
                        return json.dumps({"error": "New coding assistant must have an associated code repository."})
    
                update_op = pymongo.UpdateOne(
                    {"name": coding_assistant_upsert.name, "namespace_id": coding_assistant_upsert.auth.namespace_id},
                    {"$set": coding_assistant_upsert.exclude_auth_dict()},
                    upsert=True
                )
                operations.append(update_op)

            if operations:
                result = await self.rate_limiter.execute(
                    self.coding_assistants_collection.bulk_write,
                    operations
                )
                # Check if anything was actually modified or upserted
                if result.modified_count + result.upserted_count == 0:
                    return json.dumps({"error": "No coding assistants were upserted, no changes found!"})
                return "success"
            else:
                return json.dumps({"error": "No coding assistants were provided."})
        except PyMongoError as e:
            logging.warning(f"FunctionsAndAgentsMetadata: upsert_coding_assistants exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": f"MongoDB error occurred while upsert_coding_assistants agents: {str(e)}"})
        except Exception as e:
            logging.warning(f"FunctionsAndAgentsMetadata: upsert_coding_assistants exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": str(e)})

    async def upsert_code_repository(self, code_repository_upsert: List[UpsertCodeRepositoryInput]) -> str:
        if self.client is None or self.code_repository_collection is None or self.rate_limiter is None:
            await self.initialize()

        operations = []
        try:
            for code_repo_upsert in code_repository_upsert:
                # Check if the assistant exists
                existing_repo = await self.code_repository_collection.find_one(
                    {"name": code_repo_upsert.name, "namespace_id": code_repo_upsert.auth.namespace_id}
                )

                if not existing_repo:
                    if existing_repo.description is None:
                        return json.dumps({"error": "New code repository must have a description."})
    
                update_op = pymongo.UpdateOne(
                    {"name": code_repo_upsert.name, "namespace_id": code_repo_upsert.auth.namespace_id},
                    {"$set": code_repo_upsert.exclude_auth_dict()},
                    upsert=True
                )
                operations.append(update_op)

            if operations:
                result = await self.rate_limiter.execute(
                    self.code_repository_collection.bulk_write,
                    operations
                )
                # Check if anything was actually modified or upserted
                if result.modified_count + result.upserted_count == 0:
                    return json.dumps({"error": "No code repositories were upserted, no changes found!"})
                return "success"
            else:
                return json.dumps({"error": "No code repositories were provided."})
        except PyMongoError as e:
            logging.warning(f"FunctionsAndAgentsMetadata: upsert_code_repository exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": f"MongoDB error occurred while upsert_code_repository agents: {str(e)}"})
        except Exception as e:
            logging.warning(f"FunctionsAndAgentsMetadata: upsert_code_repository exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": str(e)})
        
    async def delete_agents(self, agents_delete: List[DeleteAgentModel]):
        if not agents_delete:
            return json.dumps({"error": "Agent delete list is empty"})
        
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()

        queries = [{"name": agent_delete.name, "namespace_id": agent_delete.auth.namespace_id} for agent_delete in agents_delete]
        try:
            # Perform the delete operations in batch
            delete_result = await self.agents_collection.delete_many({"$or": queries})
            
            if delete_result.deleted_count == 0:
                return json.dumps({"error": "No agents found or user not authorized to delete."})
            elif delete_result.deleted_count < len(agents_delete):
                return json.dumps({"error": "Some agents were not found or user not authorized to delete."})
            
            return "success"
        except Exception as e:
            logging.warning(f"FunctionsAndAgentsMetadata: delete_agents exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": str(e)})

    async def delete_code_assistants(self, code_delete: List[DeleteCodeAssistantsModel]):
        if not code_delete:
            return json.dumps({"error": "Code assistants delete list is empty"})
        
        if self.client is None or self.coding_assistants_collection is None or self.rate_limiter is None:
            await self.initialize()

        queries = [{"name": obj_delete.name, "namespace_id": obj_delete.auth.namespace_id} for obj_delete in code_delete]
        try:
            # Perform the delete operations in batch
            delete_result = await self.coding_assistants_collection.delete_many({"$or": queries})
            
            if delete_result.deleted_count == 0:
                return json.dumps({"error": "No assistants found or user not authorized to delete."})
            elif delete_result.deleted_count < len(code_delete):
                return json.dumps({"error": "Some assistants were not found or user not authorized to delete."})
            
            return "success"
        except Exception as e:
            logging.warning(f"FunctionsAndAgentsMetadata: delete_code_assistants exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": str(e)})
       
    async def delete_code_repository(self, code_delete: List[DeleteCodeRepositoryModel]):
        if not code_delete:
            return json.dumps({"error": "Code repository delete list is empty"})
        
        if self.client is None or self.code_repository_collection is None or self.rate_limiter is None:
            await self.initialize()

        queries = [{"name": obj_delete.name, "namespace_id": obj_delete.auth.namespace_id} for obj_delete in code_delete]
        try:
            # Perform the delete operations in batch
            delete_result = await self.code_repository_collection.delete_many({"$or": queries})
            
            if delete_result.deleted_count == 0:
                return json.dumps({"error": "No repositories found or user not authorized to delete."})
            elif delete_result.deleted_count < len(code_delete):
                return json.dumps({"error": "Some repositories were not found or user not authorized to delete."})
            
            return "success"
        except Exception as e:
            logging.warning(f"FunctionsAndAgentsMetadata: delete_code_repository exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": str(e)})
       
    async def delete_groups(self, group_delete: List[DeleteGroupsModel]):
        if not group_delete:
            return json.dumps({"error": "Group delete list is empty"})
        
        if self.client is None or self.coding_assistants_collection is None or self.rate_limiter is None:
            await self.initialize()

        queries = [{"name": agent_delete.name, "namespace_id": agent_delete.auth.namespace_id} for agent_delete in code_delete]
        try:
            # Perform the delete operations in batch
            delete_result = await self.coding_assistants_collection.delete_many({"$or": queries})
            
            if delete_result.deleted_count == 0:
                return json.dumps({"error": "No groups found or user not authorized to delete."})
            elif delete_result.deleted_count < len(group_delete):
                return json.dumps({"error": "Some groups were not found or user not authorized to delete."})
            
            return "success"
        except Exception as e:
            logging.warning(f"FunctionsAndAgentsMetadata: delete_groups exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": str(e)})
     
    async def get_groups(self, group_inputs: List[GetGroupModel]) -> Tuple[List[BaseGroup], str]:
        if not group_inputs:
            return [], json.dumps({"error": "Group input list is empty"})
        
        if self.client is None or self.groups_collection is None or self.rate_limiter is None:
            await self.initialize()

        try:
            unique_groups = {(group_input.name, group_input.auth.namespace_id) for group_input in group_inputs}
            names, namespace_ids = zip(*unique_groups)
            query = {"name": {"$in": names}, "namespace_id": {"$in": namespace_ids}}
            doc_cursor = self.groups_collection.find(query)
            groups_docs = await self.rate_limiter.execute(doc_cursor.to_list, length=None)
            groups = [BaseGroup(**doc) for doc in groups_docs]
            return groups, None
        except PyMongoError as e:
            logging.warning(f"FunctionsAndGroupsMetadata: get_groups exception {e}\n{traceback.format_exc()}")
            return [], json.dumps({"error": f"MongoDB error occurred while retrieving groups: {str(e)}"})
        except Exception as e:
            logging.warning(f"FunctionsAndGroupsMetadata: get_groups exception {e}\n{traceback.format_exc()}")
            return [], json.dumps({"error": str(e)})

    async def get_coding_assistants(self, code_inputs: List[GetCodingAssistantsModel]) -> Tuple[List[BaseCodingAssistant], str]:
        if not code_inputs:
            return [], json.dumps({"error": "code assistant input list is empty"})
        
        if self.client is None or self.coding_assistants_collection is None or self.rate_limiter is None:
            await self.initialize()

        try:
            unique_coding_assistants = {(code_input.name, code_input.auth.namespace_id) for code_input in code_inputs}
            names, namespace_ids = zip(*unique_coding_assistants)
            query = {"name": {"$in": names}, "namespace_id": {"$in": namespace_ids}}
            doc_cursor = self.coding_assistants_collection.find(query)
            groups_docs = await self.rate_limiter.execute(doc_cursor.to_list, length=None)
            groups = [BaseCodingAssistant(**doc) for doc in groups_docs]
            return groups, None
        except PyMongoError as e:
            logging.warning(f"FunctionsAndGroupsMetadata: get_coding_assistants exception {e}\n{traceback.format_exc()}")
            return [], json.dumps({"error": f"MongoDB error occurred while retrieving coding assistants: {str(e)}"})
        except Exception as e:
            logging.warning(f"FunctionsAndGroupsMetadata: get_coding_assistants exception {e}\n{traceback.format_exc()}")
            return [], json.dumps({"error": str(e)})

    async def get_code_repository(self, code_inputs: List[GetCodeRepositoryModel]) -> Tuple[List[BaseCodeRepository], str]:
        if not code_inputs:
            return [], json.dumps({"error": "code repository input list is empty"})
        
        if self.client is None or self.code_repository_collection is None or self.rate_limiter is None:
            await self.initialize()

        try:
            unique_code_repositories = {(code_input.name, code_input.auth.namespace_id) for code_input in code_inputs}
            names, namespace_ids = zip(*unique_code_repositories)
            query = {"name": {"$in": names}, "namespace_id": {"$in": namespace_ids}}
            doc_cursor = self.code_repository_collection.find(query)
            groups_docs = await self.rate_limiter.execute(doc_cursor.to_list, length=None)
            groups = [BaseCodeRepository(**doc) for doc in groups_docs]
            return groups, None
        except PyMongoError as e:
            logging.warning(f"FunctionsAndGroupsMetadata: get_code_repository exception {e}\n{traceback.format_exc()}")
            return [], json.dumps({"error": f"MongoDB error occurred while retrieving code repositories: {str(e)}"})
        except Exception as e:
            logging.warning(f"FunctionsAndGroupsMetadata: get_code_repository exception {e}\n{traceback.format_exc()}")
            return [], json.dumps({"error": str(e)})

    async def get_agent_groups(self, agent_names: List[str], namespace_id: str) -> Dict[str, List[str]]:
        if self.client is None or self.groups_collection is None or self.rate_limiter is None:
            await self.initialize()

        try:
            # Query the groups collection for groups that contain any of the agent names
            query = {
                "agent_names": {"$in": agent_names},
                "namespace_id": namespace_id
            }
            doc_cursor = self.groups_collection.find(query)
            groups_docs = await self.rate_limiter.execute(doc_cursor.to_list, length=None)
            
            # Create a mapping of agent names to group names
            agent_groups = {name: [] for name in agent_names}
            for doc in groups_docs:
                for agent_name in doc['agent_names']:
                    if agent_name in agent_groups:
                        agent_groups[agent_name].append(doc['name'])

            return agent_groups
        except PyMongoError as e:
            logging.warning(f"FunctionsAndAgentsMetadata: get_agent_groups exception {e}\n{traceback.format_exc()}")
            return {}
        except Exception as e:
            logging.warning(f"FunctionsAndAgentsMetadata: get_agent_groups exception {e}\n{traceback.format_exc()}")
            return {}