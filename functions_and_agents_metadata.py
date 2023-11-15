
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

class GetGroupModel(BaseModel):
    name: str
    auth: AuthAgent

class UpsertAgentInput(BaseModel):
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

class UpsertGroupInput(BaseModel):
    name: str
    auth: AuthAgent
    description: Optional[str] = None
    agents_to_add: Optional[List[str]] = None
    agents_to_remove: Optional[List[str]] = None
    
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
    capability: int = Field(default=1)
    files: Dict[str, str] = Field(default_factory=dict)

class BaseGroup(BaseModel):
    name: str = Field(default="")
    auth: AuthAgent
    description: str = Field(default="")
    agent_names: List[str] = Field(default_factory=list)
    outgoing: Dict[str, int] = Field(default_factory=dict)
    incoming: Dict[str, int] = Field(default_factory=dict)
    
class OpenAIParameter(BaseModel):
    type: str = "object"
    properties: dict = Field(default_factory=dict)
    required: List[str] = Field(default_factory=list)

class AddFunctionModel(BaseModel):
    name: str
    namespace_id: str = Field(default="")
    status: str
    description: str
    parameters: OpenAIParameter = Field(default_factory=OpenAIParameter)
    category: str
    function_code: str = Field(default="")
    class_name: str = Field(default="")

class Agent(BaseAgent):
    functions: List[AddFunctionModel] = Field(default_factory=list)

class AgentModel(BaseAgent):
    function_names: List[str] = Field(default_factory=list)

class AddFunctionInput(BaseModel):
    name: str
    auth: AuthAgent
    status: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    class_name: str = None
    parameters: OpenAIParameter = OpenAIParameter(type="object", properties={})
    function_code: Optional[str] = None
    def to_add_function_model_dict(self):
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
                self.rate_limiter = RateLimiter(rate=10, period=1)
                
            except Exception as e:
                logging.warning(f"FunctionsAndAgentsMetadata: initialize exception {e}\n{traceback.format_exc()}")

    async def do_functions_exist(self, namespace_id: str, function_names: List[str], session) -> bool:
        if self.client is None or self.funcs_collection is None or self.rate_limiter is None:
            await self.initialize()

        try:
            query = {"name": {"$in": function_names}, "namespace_id": namespace_id}
            count = await self.funcs_collection.count_documents(query, session=session)
            return count == len(function_names)
        except Exception as e:
            logging.warning(f"FunctionsAndAgentsMetadata: do_functions_exist exception {e}\n{traceback.format_exc()}")
            return False

    async def do_agents_exist(self, namespace_id: str, agent_names: List[str], session) -> bool:
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()

        try:
            query = {"name": {"$in": agent_names}, "namespace_id": namespace_id}
            count = await self.agents_collection.count_documents(query, session=session)
            return count == len(agent_names)
        except Exception as e:
            logging.warning(f"FunctionsAndAgentsMetadata: do_agents_exist exception {e}\n{traceback.format_exc()}")
            return False

    async def pull_functions(self, namespace_id: str, function_names: List[str], session=None) -> List[AddFunctionModel]:
        if self.client is None or self.funcs_collection is None or self.rate_limiter is None:
            await self.initialize()

        own_session = False
        if session is None:
            session = await self.client.start_session()
            session.start_transaction(read_concern=pymongo.read_concern.ReadConcern("snapshot"))
            own_session = True

        try:
            query = {"name": {"$in": function_names}, "namespace_id": namespace_id}
            doc_cursor = self.funcs_collection.find(query, session=session)
            docs = await self.rate_limiter.execute(doc_cursor.to_list, length=None)
            function_models = [AddFunctionModel(**doc) for doc in docs if isinstance(doc, dict)]
            
            if own_session:
                await session.commit_transaction()
                
            return function_models
        except Exception as e:
            if own_session:
                await session.abort_transaction()
            logging.warning(f"FunctionsAndAgentsMetadata: pull_functions exception {e}\n{traceback.format_exc()}")
            return []
        finally:
            if own_session:
                session.end_session()

    async def get_agents(self, agent_inputs: List[GetAgentModel], resolve_functions: bool = True) -> Tuple[List[AgentModel], str]:
        if not agent_inputs:
            return [], json.dumps({"error": "Agent input list is empty"})
        
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()

        session = await self.client.start_session()
        session.start_transaction(read_concern=pymongo.read_concern.ReadConcern("snapshot"))

        try:
            unique_agents = {(agent_input.name, agent_input.auth.namespace_id) for agent_input in agent_inputs}
            names, namespace_ids = zip(*unique_agents)
            query = {"name": {"$in": names}, "namespace_id": {"$in": namespace_ids}}
            doc_cursor = self.agents_collection.find(query, session=session)
            agents_docs = await self.rate_limiter.execute(doc_cursor.to_list, length=None)
            agents = [AgentModel(**doc) for doc in agents_docs]
            agents_dict = {(agent.name, agent.auth.namespace_id): agent for agent in agents}
            all_function_names = set()
            for agent in agents:
                all_function_names.update(agent.function_names)
            
            functions_dict = {}
            if resolve_functions and all_function_names:
                functions = await self.pull_functions(namespace_ids[0], list(all_function_names), session=session)
                functions_dict = {function.name: function for function in functions}

            results = []
            for agent_input in agent_inputs:
                key = (agent_input.name, agent_input.auth.namespace_id)
                agent_model = agents_dict.get(key)
                if agent_model:  # Check if the agent_model exists
                    if resolve_functions and agent_model.function_names:
                        agent = Agent(**agent_model.dict())
                        agent.functions = [functions_dict[name] for name in agent_model.function_names if name in functions_dict]
                        results.append(agent)
                    else:
                        results.append(agent_model)
            await session.commit_transaction()
            return results, None
        except PyMongoError as e:
            await session.abort_transaction()
            logging.warning(f"FunctionsAndAgentsMetadata: get_agents exception {e}\n{traceback.format_exc()}")
            return [], json.dumps({"error": f"MongoDB error occurred while retrieving agents: {str(e)}"})
        except Exception as e:
            await session.abort_transaction()
            logging.warning(f"FunctionsAndAgentsMetadata: get_agents exception {e}\n{traceback.format_exc()}")
            return [], json.dumps({"error": f"Error occurred while retrieving agents: {str(e)}"})
        finally:
            session.end_session()

    async def update_comms(self, agent_input: UpdateComms):
        if self.client is None or self.groups_collection is None or self.rate_limiter is None:
            await self.initialize()

        async with await self.client.start_session() as session:
            session.start_transaction()
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
                    await self.groups_collection.bulk_write(operations, session=session)
                
                await session.commit_transaction()
                return "success"
            except PyMongoError as e:
                await session.abort_transaction()
                logging.warning(f"update_comms exception {e}\n{traceback.format_exc()}")
                return json.dumps({"error": f"MongoDB error occurred: {str(e)}"})
            finally:
                session.end_session()

    async def upsert_functions(self, functions: List[AddFunctionInput]) -> str:
        if self.client is None or self.funcs_collection is None or self.rate_limiter is None:
            await self.initialize()

        session = await self.client.start_session()
        operations = []
        try:
            session.start_transaction()
            for function in functions:
                # Check if the function exists
                existing_function = await self.funcs_collection.find_one(
                    {"name": function.name, "namespace_id": function.auth.namespace_id},
                    session=session
                )
                if not existing_function and function.category is None:
                    await session.abort_transaction()
                    return json.dumps({"error": "New functions must have a category defined."})
                if not existing_function and function.status is None:
                    await session.abort_transaction()
                    return json.dumps({"error": "New functions must have a status defined."})
                if not existing_function and function.status == "accepted":
                    await session.abort_transaction()
                    return json.dumps({"error": "New untested functions cannot have an accepted status."})
                if not existing_function and function.description is None:
                    await session.abort_transaction()
                    return json.dumps({"error": "New functions must have a description defined."})
                if not existing_function and function.class_name is None and function.function_code is None:
                    await session.abort_transaction()
                    return json.dumps({"error": "New functions must have either function_code or class_name defined."})
                function_model_data = function.to_add_function_model_dict()
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
                    operations,
                    session=session
                )
                await session.commit_transaction()

                # Check if anything was actually modified or upserted
                if result.modified_count + result.upserted_count == 0:
                    await session.abort_transaction()
                    return json.dumps({"error": "No functions were upserted, no changes found!"})
                return "success"
        except PyMongoError as e:
            await session.abort_transaction()
            logging.warning(f"FunctionsAndAgentsMetadata: upsert_functions exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": f"MongoDB error occurred while upserting functions: {str(e)}"})
        except Exception as e:
            await session.abort_transaction()
            return json.dumps({"error": str(e)})
        finally:
            session.end_session()

    async def upsert_agents(self, agents_upsert: List[UpsertAgentInput]) -> str:
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()

        operations = []
        session = await self.client.start_session()
        try:
            session.start_transaction()
            for agent_upsert in agents_upsert:
                # Check if the agent exists
                existing_agent = await self.agents_collection.find_one(
                    {"name": agent_upsert.name, "namespace_id": agent_upsert.auth.namespace_id},
                    session=session
                )

                # If it's a new agent and no category is provided, fail the operation
                if not existing_agent and agent_upsert.category is None:
                    await session.abort_transaction()
                    return json.dumps({"error": "New agents must have a category defined."})
                if agent_upsert.functions_to_add:
                    if not await self.do_functions_exist(agent_upsert.auth.namespace_id, agent_upsert.functions_to_add, session):
                        return json.dumps({"error": "One of the functions you are trying to add does not exist"})
                # Generate the update dictionary using Pydantic's .dict() method
                update_data = agent_upsert.dict(exclude_none=True, exclude={'functions_to_add', 'functions_to_remove', 'files_to_add', 'files_to_remove'})
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
                    operations,
                    session=session
                )
                # Check if anything was actually modified or upserted
                if result.modified_count + result.upserted_count == 0:
                    await session.abort_transaction()
                    return json.dumps({"error": "No agents were upserted, no changes found!"})
                await session.commit_transaction()
                return "success"
            else:
                await session.abort_transaction()
                return "No agents were provided."
        except PyMongoError as e:
            await session.abort_transaction()
            logging.warning(f"FunctionsAndAgentsMetadata: upsert_agents exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": f"MongoDB error occurred while upserting agents: {str(e)}"})
        except Exception as e:
            await session.abort_transaction()
            logging.warning(f"FunctionsAndAgentsMetadata: upsert_agents exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": str(e)})
        finally:
            session.end_session()

    async def upsert_groups(self, groups_upsert: List[UpsertGroupInput]) -> str:
        if self.client is None or self.groups_collection is None or self.rate_limiter is None:
            await self.initialize()

        operations = []
        session = await self.client.start_session()
        try:
            session.start_transaction()
            for group_upsert in groups_upsert:
                if group_upsert.agents_to_add:
                    if not await self.do_agents_exist(group_upsert.auth.namespace_id, group_upsert.agents_to_add, session):
                        return json.dumps({"error": "One of the agents you are trying to add does not exist"})

                query = {
                    "name": group_upsert.name, 
                    "namespace_id": group_upsert.auth.namespace_id
                }
                update = {
                    "$set": {k: v for k, v in group_upsert.dict(exclude_none=True).items() if k not in ['agents_to_add', 'agents_to_remove']},
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
                    operations,
                    session=session
                )
                # Check if anything was actually modified or upserted
                if result.modified_count + result.upserted_count == 0:
                    await session.abort_transaction()
                    return json.dumps({"error": "No groups were upserted, no changes found!"})
                await session.commit_transaction()
                return "success"
            else:
                await session.abort_transaction()
                return json.dumps({"error": "No groups were provided."})
        except PyMongoError as e:
            await session.abort_transaction()
            logging.warning(f"FunctionsAndAgentsMetadata: upsert_groups exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": f"MongoDB error occurred while upsert_groups agents: {str(e)}"})
        except Exception as e:
            await session.abort_transaction()
            logging.warning(f"FunctionsAndAgentsMetadata: upsert_groups exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": str(e)})
        finally:
            session.end_session()

    async def delete_agents(self, agents_delete: List[DeleteAgentModel]):
        if not agents_delete:
            return json.dumps({"error": "Agent delete list is empty"})
        
        if self.client is None or self.agents_collection is None or self.rate_limiter is None:
            await self.initialize()

        queries = [{"name": agent_delete.name, "namespace_id": agent_delete.auth.namespace_id} for agent_delete in agents_delete]
        session = await self.client.start_session()
        try:
            async with session.start_transaction():
                # Perform the delete operations in batch
                delete_result = await self.agents_collection.delete_many({"$or": queries}, session=session)
                
                if delete_result.deleted_count == 0:
                    return json.dumps({"error": "No agents found or user not authorized to delete."})
                elif delete_result.deleted_count < len(agents_delete):
                    return json.dumps({"error": "Some agents were not found or user not authorized to delete."})
                
                return "success"
        except Exception as e:
            logging.warning(f"FunctionsAndAgentsMetadata: delete_agents exception {e}\n{traceback.format_exc()}")
            return json.dumps({"error": str(e)})
        finally:
            session.end_session()

    async def get_groups(self, group_inputs: List[GetGroupModel]) -> Tuple[List[BaseGroup], str]:
        if not group_inputs:
            return [], json.dumps({"error": "Group input list is empty"})
        
        if self.client is None or self.groups_collection is None or self.rate_limiter is None:
            await self.initialize()

        session = await self.client.start_session()
        session.start_transaction(read_concern=pymongo.read_concern.ReadConcern("snapshot"))

        try:
            unique_groups = {(group_input.name, group_input.auth.namespace_id) for group_input in group_inputs}
            names, namespace_ids = zip(*unique_groups)
            query = {"name": {"$in": names}, "namespace_id": {"$in": namespace_ids}}
            doc_cursor = self.groups_collection.find(query, session=session)
            groups_docs = await self.rate_limiter.execute(doc_cursor.to_list, length=None)
            groups = [BaseGroup(**doc) for doc in groups_docs]
            await session.commit_transaction()
            return groups, None
        except PyMongoError as e:
            await session.abort_transaction()
            logging.warning(f"FunctionsAndGroupsMetadata: get_groups exception {e}\n{traceback.format_exc()}")
            return [], json.dumps({"error": f"MongoDB error occurred while retrieving groups: {str(e)}"})
        except Exception as e:
            await session.abort_transaction()
            logging.warning(f"FunctionsAndGroupsMetadata: get_groups exception {e}\n{traceback.format_exc()}")
            return [], json.dumps({"error": str(e)})
        finally:
            session.end_session()
 
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