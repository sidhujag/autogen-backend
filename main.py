import os
import logging

from dotenv import load_dotenv
from fastapi import FastAPI
from discover_functions_manager import DiscoverFunctionsManager, DiscoverFunctionsModel
from discover_agents_manager import DiscoverAgentsManager, DiscoverAgentsModel
from functions_and_agents_metadata import FunctionsAndAgentsMetadata, AddFunctionInput, GetAgentModel, DeleteAgentModel, UpsertAgentInput
from cachetools import TTLCache
from rate_limiter import RateLimiter, SyncRateLimiter
from typing import List
rate_limiter = RateLimiter(rate=5, period=1)  # Allow 5 tasks per second
rate_limiter_sync = SyncRateLimiter(rate=5, period=1)
# Load environment variables
load_dotenv()

app = FastAPI()

# Initialize logging
LOGFILE_PATH = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), 'app.log')
logging.basicConfig(filename=LOGFILE_PATH, filemode='w',
                    format='%(asctime)s.%(msecs)03d %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S', force=True, level=logging.INFO)


discover_functions_manager = DiscoverFunctionsManager(rate_limiter, rate_limiter_sync)
discover_agents_manager = DiscoverAgentsManager(rate_limiter, rate_limiter_sync)
functions_and_agents_metadata = FunctionsAndAgentsMetadata()

discoverfunctioncache = TTLCache(maxsize=16384, ttl=36000)
discoveragentscache = TTLCache(maxsize=16384, ttl=36000)

@app.post('/discover_functions/')
async def discoverFunctions(function_input: DiscoverFunctionsModel):
    """Endpoint to get functions based on provided input."""
    if function_input.auth.api_key == '':
        return {'response': "Error: LLM API key not provided", 'elapsed_time': 0}
    if function_input.auth.namespace_id == '':
        return {'response': "Error: namespace_id not provided", 'elapsed_time': 0}
    result = discoverfunctioncache.get(function_input)
    if result is not None:
        logging.info(f'Found functions in cache, result {result}')
        return {'response': result, 'elapsed_time': 0}
    function_types = ['information_retrieval', 'communication', 'data_processing', 'sensory_perception', 'programming']

    if function_input.category not in function_types:
        return {'response': f'Invalid category {function_input.category}, must be one of {function_types}'}

    logging.info(f'Discovering function: {function_input}')
    result, elapsed_time = await discover_functions_manager.pull_functions(function_input)
    if len(result) > 0:
        discoverfunctioncache[function_input] = result
    return {'response': result, 'elapsed_time': elapsed_time}

@app.post('/add_functions/')
async def addFunction(function_inputs: List[AddFunctionInput]):
    """Endpoint to push functions based on provided functions."""
    if len(function_inputs) == 0:
        return {'response': "Error: No functions provided", 'elapsed_time': 0}
    for function_input in function_inputs:
        if function_input.auth.api_key == '':
            return {'response': "Error: LLM API key not provided", 'elapsed_time': 0}
        if function_input.auth.namespace_id == '':
            return {'response': "Error: namespace_id not provided", 'elapsed_time': 0}
        logging.info(f'Adding function: {function_input.name}')
        functions = {}
        function_types = ['information_retrieval', 'communication', 'data_processing', 'sensory_perception', 'programming']

        if function_input.category not in function_types:
            return {'response': f'Invalid category for function {function_input.name}, must be one of {function_types}'}

        # Append the new function to the category
        new_function = {
            'name': function_input.name,
            'description': function_input.description
        }
        functions[function_input.category] = [new_function]

    # Push the functions
    result, elapsed_time1 = await functions_and_agents_metadata.set_functions(function_inputs)
    if result != "success":
        return {'response': result, 'elapsed_time': elapsed_time1}
    result, elapsed_time2 = await discover_functions_manager.push_functions(function_input.auth, functions)
    return {'response': result, 'elapsed_time': elapsed_time1+elapsed_time2}

@app.post('/get_agent/')
async def getAgent(agent_input: GetAgentModel):
    """Endpoint to get agent."""
    if agent_input.auth.api_key == '':
        return {'response': "Error: LLM API key not provided", 'elapsed_time': 0}
    if agent_input.auth.namespace_id == '':
        return {'response': "Error: namespace_id not provided", 'elapsed_time': 0}
    if agent_input.name == '':
        return {'response': "Error: agent name not provided!", 'elapsed_time': 0}
    response, elapsed_time = await functions_and_agents_metadata.get_agent(agent_input)
    if len(response.name) == 0:
        agent_input.auth.namespace_id = ""
        response, elapsed_time = await functions_and_agents_metadata.get_agent(agent_input)
    return {'response': response, 'elapsed_time': elapsed_time}

@app.post('/upsert_agent/')
async def upsertAgent(agent_input: UpsertAgentInput):
    """Endpoint to upsert agent."""
    if agent_input.auth.api_key == '':
        return {'response': "Error: LLM API key not provided", 'elapsed_time': 0}
    if agent_input.auth.namespace_id == '':
        return {'response': "Error: namespace_id not provided", 'elapsed_time': 0}
    if agent_input.name == '':
        return {'response': "Error: agent name not provided!", 'elapsed_time': 0}
    agents = {}
    if agent_input.category:
        agent_types = ['information_retrieval', 'communication', 'data_processing', 'sensory_perception', 'programming', 'planning', 'groups', 'user']
        if agent_input.category not in agent_types:
            return {'response': f'Invalid category for agent {agent_input.name}, must be one of {agent_types}'}
    if agent_input.human_input_mode:
        human_input_types = ['ALWAYS', 'NEVER', 'TERMINATE']
        if agent_input.human_input_mode not in human_input_types:
            return {'response': f'Invalid human_input_mode for agent {agent_input.human_input_mode}, must be one of {human_input_types}'}
    # Push the agent
    response, elapsed_time1, agent = await functions_and_agents_metadata.upsert_agent(agent_input)
    if response != "success":
        return {'response': response, 'elapsed_time': elapsed_time1}
    discoveragentscache.clear()
    # Append the new agent to the category
    new_agent = {
        'name': agent.name,
        'description': agent.description
    }
    agents[agent.category] = [new_agent]
    result, elapsed_time2 = await discover_agents_manager.push_agents(agent_input.auth, agents)
    return {'response': result, 'elapsed_time': elapsed_time1+elapsed_time2}

@app.post('/discover_agents/')
async def discoverAgents(agent_input: DiscoverAgentsModel):
    """Endpoint to upsert an agent."""
    if agent_input.auth.api_key == '':
        return {'response': "Error: LLM API key not provided", 'elapsed_time': 0}
    if agent_input.auth.namespace_id == '':
        return {'response': "Error: namespace_id not provided", 'elapsed_time': 0}
    agent_types = ['information_retrieval', 'communication', 'data_processing', 'sensory_perception', 'programming', 'planning', 'groups', 'user']

    if agent_input.category not in agent_types:
        return {'response': f'Invalid category {agent_input.category}, must be one of {agent_types}'}

    if agent_input.query:
        result = discoveragentscache.get(agent_input.query)
        if result is not None:
            logging.info(f'Found agents in cache, result {result}')
            return {'response': result, 'elapsed_time': 0}
    result, elapsed_time = await discover_agents_manager.pull_agents(agent_input)
    if len(result) > 0 and agent_input.query:
        discoveragentscache[agent_input.query] = result
    return {'response': result, 'elapsed_time': elapsed_time}

@app.post('/delete_agent/')
async def deleteAgent(agent_input: DeleteAgentModel):
    """Endpoint to delete agent."""
    if agent_input.auth.api_key == '':
        return {'response': "Error: LLM API key not provided", 'elapsed_time': 0}
    if agent_input.auth.namespace_id == '':
        return {'response': "Error: namespace_id not provided", 'elapsed_time': 0}
    if agent_input.name == '':
        return {'response': "Error: agent name not provided!", 'elapsed_time': 0}
    logging.info(f'Deleting agent: {agent_input.name}')
    # delete the agent
    response, elapsed_time1 = await functions_and_agents_metadata.delete_agent(agent_input)
    if response != "success":
        return {'response': response, 'elapsed_time': elapsed_time1}
    discoveragentscache.clear()
    result, elapsed_time2 = discover_agents_manager.delete_agent(agent_input.auth, agent_input.name)
    return {'response': result, 'elapsed_time': elapsed_time1+elapsed_time2}
