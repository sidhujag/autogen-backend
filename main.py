import os
import logging
import time

from dotenv import load_dotenv
from fastapi import FastAPI
from discover_functions_manager import DiscoverFunctionsManager, DiscoverFunctionsModel
from discover_agents_manager import DiscoverAgentsManager, DiscoverAgentsModel
from discover_groups_manager import DiscoverGroupsManager, DiscoverGroupsModel
from functions_and_agents_metadata import FunctionsAndAgentsMetadata, GetGroupModel, UpsertGroupInput, UpdateComms, AddFunctionInput, GetAgentModel, DeleteAgentModel, UpsertAgentInput
from rate_limiter import RateLimiter, SyncRateLimiter
from typing import List
rate_limiter = RateLimiter(rate=10, period=1)  # Allow 5 tasks per second
rate_limiter_sync = SyncRateLimiter(rate=10, period=1)
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
discover_groups_manager = DiscoverGroupsManager(rate_limiter, rate_limiter_sync)
functions_and_agents_metadata = FunctionsAndAgentsMetadata()


@app.post('/discover_functions/')
async def discoverFunctions(function_input: DiscoverFunctionsModel):
    """Endpoint to get functions based on provided input."""
    start = time.time()
    if function_input.auth.api_key == '':
        return {'response': "Error: LLM API key not provided", 'elapsed_time': 0}
    if function_input.auth.namespace_id == '':
        return {'response': "Error: namespace_id not provided", 'elapsed_time': 0}
    function_types = ['information_retrieval', 'communication', 'data_processing', 'sensory_perception', 'programming', 'planning']

    if function_input.category != "" and function_input.category not in function_types:
        return {'response': f'Invalid category {function_input.category}, must be one of {function_types}'}

    logging.info(f'Discovering function: {function_input}')
    result = await discover_functions_manager.pull_functions(function_input)
    end = time.time()
    return {'response': result, 'elapsed_time': end-start}

@app.post('/upsert_functions/')
async def upsertFunctions(function_inputs: List[AddFunctionInput]):
    """Endpoint to upsert functions based on provided functions."""
    start = time.time()
    if len(function_inputs) == 0:
        return {'response': "Error: No functions provided", 'elapsed_time': 0}
    functions = {}
    for function_input in function_inputs:
        if function_input.auth.api_key == '':
            return {'response': "Error: LLM API key not provided", 'elapsed_time': 0}
        if function_input.auth.namespace_id == '':
            return {'response': "Error: namespace_id not provided", 'elapsed_time': 0}
        function_types = ['information_retrieval', 'communication', 'data_processing', 'sensory_perception', 'programming', 'planning']

        if function_input.category and function_input.category not in function_types:
            return {'response': f'Invalid category for function {function_input.name}, must be one of {function_types}'}

        if function_input.description:
            # Append the new function to the category
            new_function = {
                'name': function_input.name,
                'description': function_input.description
            }
            if function_input.category in functions:
                functions[function_input.category].append(new_function)
            else:
                functions[function_input.category] = [new_function]

    # Push the functions
    result = await functions_and_agents_metadata.upsert_functions(function_inputs)
    if result != "success":
        end = time.time()
        return {'response': result, 'elapsed_time': end-start}
    if len(functions) > 0:
        result = await discover_functions_manager.push_functions(function_input.auth, functions)
    end = time.time()
    return {'response': result, 'elapsed_time': end-start}

@app.post('/get_agents/')
async def getAgents(agent_inputs: List[GetAgentModel]):
    """Endpoint to get agent."""
    start = time.time()
    if len(agent_inputs) == 0:
        return {'response': "Error: No agents provided", 'elapsed_time': 0}
    for agent in agent_inputs:
        if agent.auth.api_key == '':
            return {'response': "Error: LLM API key not provided", 'elapsed_time': 0}
        if agent.auth.namespace_id == '':
            return {'response': "Error: namespace_id not provided", 'elapsed_time': 0}
        if agent.name == '':
            return {'response': "Error: agent name not provided!", 'elapsed_time': 0}
    response, err = await functions_and_agents_metadata.get_agents(agent_inputs)
    if err is not None:
        for agent in agent_inputs:
            agent.auth.namespace_id = ""
        response, err = await functions_and_agents_metadata.get_agents(agent_inputs)
        if err is not None:
            response = err
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}

@app.post('/upsert_agents/')
async def upsertAgents(agent_inputs: List[UpsertAgentInput]):
    """Endpoint to upsert agent."""
    if len(agent_inputs) == 0:
        return {'response': "Error: No agents provided", 'elapsed_time': 0}
    start = time.time()
    agents = {}
    for agent_input in agent_inputs:
        if agent_input.auth.api_key == '':
            return {'response': "Error: LLM API key not provided", 'elapsed_time': 0}
        if agent_input.auth.namespace_id == '':
            return {'response': "Error: namespace_id not provided", 'elapsed_time': 0}
        if agent_input.name == '':
            return {'response': "Error: agent name not provided!", 'elapsed_time': 0}
        if agent_input.description and agent_input.description == '':
            return {'response': "Error: agent description not provided!", 'elapsed_time': 0}
        if agent_input.category:
            agent_types = ['information_retrieval', 'communication', 'data_processing', 'sensory_perception', 'programming', 'planning', 'user']
            if agent_input.category not in agent_types:
                return {'response': f'Invalid category for agent {agent_input.name}, must be one of {agent_types}', 'elapsed_time': 0}
        if agent_input.human_input_mode:
            human_input_types = ['ALWAYS', 'NEVER', 'TERMINATE']
            if agent_input.human_input_mode not in human_input_types:
                return {'response': f'Invalid human_input_mode for agent {agent_input.human_input_mode}, must be one of {human_input_types}', 'elapsed_time': 0}
        if agent_input.type:
            types = ['BASIC', 'FULL']
            if agent_input.type not in types:
                return {'response': f'Invalid type for agent {agent_input.type}, must be one of {types}', 'elapsed_time': 0}
    # Push the agent
    response = await functions_and_agents_metadata.upsert_agents(agent_inputs)
    if response != "success":
        end = time.time()
        return {'response': response, 'elapsed_time': end-start}
    for agent_input in agent_inputs:
        if not agent_input.description or not agent_input.category:
            continue
        # Append the new agent to the category
        new_agent = {
            'name': agent_input.name,
            'description': agent_input.description
        }
        if agent_input.category in agents:
            agents[agent_input.category].append(new_agent)
        else:
            agents[agent_input.category] = [new_agent]
    
    if len(agents) > 0:
        response = await discover_agents_manager.push_agents(agent_inputs[0].auth, agents)
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}


@app.post('/update_communication_stats/')
async def updateComms(comms_input: UpdateComms):
    """Endpoint to update communication (incoming/outgoing) stats an agent."""
    start = time.time()
    if not comms_input.sender or not comms_input.receiver:
        return {'response': "Error: sender and receiver not provided", 'elapsed_time': 0}
    if comms_input.auth.namespace_id == '':
        return {'response': "Error: namespace_id not provided", 'elapsed_time': 0}
    response = await functions_and_agents_metadata.update_comms(comms_input)
    if response != "success":
        end = time.time()
        return {'response': response, 'elapsed_time': end-start}
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}

@app.post('/discover_agents/')
async def discoverAgents(agent_input: DiscoverAgentsModel):
    """Endpoint to discover agents."""
    start = time.time()
    if agent_input.auth.api_key == '':
        return {'response': "Error: LLM API key not provided", 'elapsed_time': 0}
    if agent_input.auth.namespace_id == '':
        return {'response': "Error: namespace_id not provided", 'elapsed_time': 0}
    agent_types = ['information_retrieval', 'communication', 'data_processing', 'sensory_perception', 'programming', 'planning', 'user']

    if agent_input.category != "" and agent_input.category not in agent_types:
        return {'response': f'Invalid category {agent_input.category}, must be one of {agent_types}'}

    result = await discover_agents_manager.pull_agents(agent_input)
    
    # Flatten the result if it's a list of lists
    if result and isinstance(result[0], list):
        result = [item for sublist in result for item in sublist]

    # Extract agent names from the result
    agent_names = [agent['name'] for agent in result if 'name' in agent]

    # Get the groups for these agents
    agent_groups = await functions_and_agents_metadata.get_agent_groups(agent_names, agent_input.auth.namespace_id)

    # Add the group names to the result
    for agent in result:
        agent['group_names'] = agent_groups.get(agent['name'], [])

    end = time.time()
    return {'response': result, 'elapsed_time': end-start}

@app.post('/discover_groups/')
async def discoverGroups(group_input: DiscoverGroupsModel):
    """Endpoint to discover groups."""
    start = time.time()
    if group_input.auth.api_key == '':
        return {'response': "Error: LLM API key not provided", 'elapsed_time': 0}
    if group_input.auth.namespace_id == '':
        return {'response': "Error: namespace_id not provided", 'elapsed_time': 0}

    result = await discover_groups_manager.pull_groups(group_input)
    end = time.time()
    return {'response': result, 'elapsed_time': end-start}

@app.post('/get_groups/')
async def getGroups(group_inputs: List[GetGroupModel]):
    """Endpoint to get group info."""
    start = time.time()
    if len(group_inputs) == 0:
        return {'response': "Error: No groups provided", 'elapsed_time': 0}
    for group_input in group_inputs:
        if group_input.auth.api_key == '':
            return {'response': "Error: LLM API key not provided", 'elapsed_time': 0}
        if group_input.auth.namespace_id == '':
            return {'response': "Error: namespace_id not provided", 'elapsed_time': 0}
        if group_input.name == '':
            return {'response': "Error: group name not provided!", 'elapsed_time': 0}
    response, err = await functions_and_agents_metadata.get_groups(group_inputs)
    if err is not None:
        for group in group_inputs:
            group.auth.namespace_id = ""
        response, err = await functions_and_agents_metadata.get_groups(group_inputs)
        if err is not None:
            response = err
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}

@app.post('/upsert_groups/')
async def upsertGroups(group_inputs: List[UpsertGroupInput]):
    """Endpoint to upsert group."""
    if len(group_inputs) == 0:
        return {'response': "Error: No groups provided", 'elapsed_time': 0}
    start = time.time()
    groups = []
    for group_input in group_inputs:
        if group_input.auth.api_key == '':
            return {'response': "Error: LLM API key not provided", 'elapsed_time': 0}
        if group_input.auth.namespace_id == '':
            return {'response': "Error: namespace_id not provided", 'elapsed_time': 0}
        if group_input.name == '':
            return {'response': "Error: group name not provided!", 'elapsed_time': 0}
        if group_input.description and group_input.description == '':
            return {'response': "Error: group description not provided!", 'elapsed_time': 0}

    # Push the group
    response = await functions_and_agents_metadata.upsert_groups(group_inputs)
    if response != "success":
        end = time.time()
        return {'response': response, 'elapsed_time': end-start}
    for group_input in group_inputs:
        if not group_input.description:
            continue
        new_group = {
            'name': group_input.name,
            'description': group_input.description
        }
        groups.append(new_group)
    
    if len(groups) > 0:
        response = await discover_groups_manager.push_groups(group_inputs[0].auth, groups)
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}


@app.post('/delete_agents/')
async def deleteAgent(agent_inputs: List[DeleteAgentModel]):
    """Endpoint to delete agent."""
    start = time.time()
    for agent_input in agent_inputs:
        if agent_input.auth.api_key == '':
            return {'response': "Error: LLM API key not provided", 'elapsed_time': 0}
        if agent_input.auth.namespace_id == '':
            return {'response': "Error: namespace_id not provided", 'elapsed_time': 0}
        if agent_input.name == '':
            return {'response': "Error: agent name not provided!", 'elapsed_time': 0}
        logging.info(f'Deleting agent: {agent_input.name}')
    # delete the agent
    response = await functions_and_agents_metadata.delete_agents(agent_inputs)
    if response != "success":
        end = time.time()
        return {'response': response, 'elapsed_time': end-start}
    result = discover_agents_manager.delete_agents(agent_inputs)
    end = time.time()
    return {'response': result, 'elapsed_time': end-start}
